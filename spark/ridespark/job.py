"""Job entry point: read raw data, curate, write Parquet. Exit codes:
0 ok, 1 unexpected error, 2 bad arguments, 3 required dimension unavailable,
4 schema mismatch, 5 driver-history integrity problem."""
from __future__ import annotations

import logging
import sys

from pyspark.sql import DataFrame, SparkSession

from .config import JobConfig, JobConfigError, parse_args
from .enrich import DimensionError, DimensionUnavailableError, prepare_zones
from .io import (
    materialize, read_latest_scd2, read_source, write_metrics, write_rejected, write_rides, write_scd2,
    write_snapshot_conflicts,
)
from .normalize import build_unified
from .pipeline import RunContext, compute_metrics, process_rides
from .scd2 import Scd2Error, apply_snapshot, empty_history, prepare_snapshot, validate_scd2
from .schemas import SchemaMismatchError
from .session import build_session

logger = logging.getLogger("ridespark.job")

EXIT_OK, EXIT_ERROR, EXIT_CONFIG, EXIT_DIMENSION, EXIT_SCHEMA, EXIT_INTEGRITY = 0, 1, 2, 3, 4, 5


def _unavailable(cfg: JobConfig, reason: str) -> None:
    if not cfg.allow_missing_dimensions:
        raise DimensionUnavailableError(
            f"{reason}. Nothing was written. Provide the dimension, or rerun with --allow-missing-dimensions "
            "to continue with null attributes and *_match = DIMENSION_UNAVAILABLE."
        )
    logger.warning("continuing without dimension: %s", reason)


def load_zones(spark: SparkSession, cfg: JobConfig) -> DataFrame | None:
    if not cfg.zones_input:
        _unavailable(cfg, "zone dimension not configured (--zones-input)")
        return None
    try:
        return prepare_zones(read_source(spark, cfg.zones_input))
    except SchemaMismatchError:
        raise
    except DimensionUnavailableError as exc:
        _unavailable(cfg, f"zone dimension unusable: {exc}")
    except DimensionError:
        raise
    except Exception as exc:  # noqa: BLE001 - unreadable source counts as unavailable
        _unavailable(cfg, f"cannot read zone dimension {cfg.zones_input}: {type(exc).__name__}")
    return None


def load_drivers(spark: SparkSession, cfg: JobConfig) -> tuple[DataFrame | None, int]:
    """Apply today's snapshot when given (and store the new history), else use the stored history."""
    existing = read_latest_scd2(spark, cfg.output_path)
    if not cfg.driver_snapshot:
        if existing is None:
            _unavailable(cfg, "no driver snapshot given and no stored driver history")
        return existing, 0
    try:
        snapshot = read_source(spark, cfg.driver_snapshot)
        _, conflicts = prepare_snapshot(snapshot)
        history = apply_snapshot(existing if existing is not None else empty_history(spark), snapshot, cfg.snapshot_date)
        validate_scd2(history)
    except (SchemaMismatchError, Scd2Error):
        raise
    except Exception as exc:  # noqa: BLE001
        _unavailable(cfg, f"cannot use driver snapshot {cfg.driver_snapshot}: {type(exc).__name__}")
        return existing, 0
    history = materialize(spark, history)
    conflict_count = conflicts.count()
    write_scd2(history, cfg.output_path, cfg.snapshot_date)
    if conflict_count:
        logger.warning("%d driver snapshot row(s) ignored as conflicting", conflict_count)
        write_snapshot_conflicts(conflicts, cfg.output_path, cfg.snapshot_date)
    return history, conflict_count


def run(spark: SparkSession, cfg: JobConfig) -> dict:
    stream = read_source(spark, cfg.stream_input) if cfg.stream_input else None
    hist = read_source(spark, cfg.hist_input) if cfg.hist_input else None
    unified = build_unified(stream, hist)

    zones = load_zones(spark, cfg)
    drivers, conflict_count = load_drivers(spark, cfg)

    ctx = RunContext(cfg.processing_date, cfg.pickup_start, cfg.pickup_end, cfg.timezone, cfg.run_id)
    result = process_rides(unified, zones, drivers, ctx)
    result.curated.cache()
    result.rejected.cache()
    metrics = compute_metrics(result)
    metrics.update(run_id=cfg.run_id, processing_date=cfg.processing_date, pickup_start=cfg.pickup_start,
                   pickup_end=cfg.pickup_end, driver_snapshot_conflicts=conflict_count)

    write_rides(result.curated, cfg.output_path)
    write_rejected(result.rejected, cfg.output_path)
    write_metrics(spark, cfg.output_path, cfg.processing_date, cfg.run_id, metrics)
    return metrics


def main(argv: list[str] | None = None, spark: SparkSession | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format='{"ts":"%(asctime)s","severity":"%(levelname)s","message":"%(message)s"}')
    try:
        cfg = parse_args(argv)
    except JobConfigError as exc:
        for problem in exc.problems:
            print(f"Argument error: {problem}", file=sys.stderr)
        return EXIT_CONFIG
    except SystemExit as exc:  # argparse already printed usage
        return EXIT_CONFIG if exc.code else EXIT_OK

    try:
        metrics = run(spark or build_session(), cfg)
    except DimensionUnavailableError as exc:
        logger.error("dimension unavailable: %s", exc)
        return EXIT_DIMENSION
    except (SchemaMismatchError, DimensionError) as exc:
        logger.error("schema or dimension problem: %s", exc)
        return EXIT_SCHEMA
    except Scd2Error as exc:
        logger.error("driver history problem: %s", exc)
        return EXIT_INTEGRITY
    except Exception:  # noqa: BLE001
        logger.exception("job failed")
        return EXIT_ERROR
    logger.info("run complete: %s", metrics)
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
