"""Explicit schemas for every input and output of the processing job (no schema inference)."""
from __future__ import annotations

from pyspark.sql import DataFrame, types as T
from pyspark.sql import functions as F

MONEY = T.DecimalType(38, 9)  # BigQuery NUMERIC


def _struct(*cols: tuple[str, T.DataType]) -> T.StructType:
    return T.StructType([T.StructField(name, dtype, True) for name, dtype in cols])


# ---- Inputs (raw layer written by the ingestion code) ----
STREAM_RAW_SCHEMA = _struct(
    ("ride_id", T.StringType()), ("driver_id", T.StringType()), ("rider_id", T.StringType()),
    ("pickup_zone", T.StringType()), ("dropoff_zone", T.StringType()),
    ("pickup_ts", T.TimestampType()), ("dropoff_ts", T.TimestampType()),
    ("fare", MONEY), ("surge_multiplier", T.DoubleType()), ("ingest_ts", T.TimestampType()),
)

# Only the NYC TLC 2018 columns the job uses; other raw columns are ignored.
HIST_RAW_SCHEMA = _struct(
    ("VendorID", T.LongType()),
    ("tpep_pickup_datetime", T.TimestampType()), ("tpep_dropoff_datetime", T.TimestampType()),
    ("trip_distance", T.DoubleType()),
    ("PULocationID", T.LongType()), ("DOLocationID", T.LongType()),
    ("fare_amount", MONEY),
    ("source_file", T.StringType()), ("ingest_ts", T.TimestampType()),
)

ZONE_SCHEMA = _struct(
    ("zone_id", T.StringType()), ("zone_name", T.StringType()),
    ("borough", T.StringType()), ("service_zone", T.StringType()),
)
# NYC TLC taxi_zone_lookup.csv header -> canonical names
ZONE_COLUMN_ALIASES = {"LocationID": "zone_id", "Zone": "zone_name", "Borough": "borough"}

# Daily driver snapshot: one row per driver per day. Attributes come only from this source.
DRIVER_SNAPSHOT_SCHEMA = _struct(
    ("driver_id", T.StringType()), ("vehicle_type", T.StringType()),
    ("home_zone", T.StringType()), ("status", T.StringType()),
)
DRIVER_TRACKED_COLUMNS = ("vehicle_type", "home_zone", "status")

# ---- Outputs ----
UNIFIED_SCHEMA = _struct(
    ("ride_id", T.StringType()), ("source", T.StringType()), ("source_file", T.StringType()),
    ("driver_id", T.StringType()), ("rider_id", T.StringType()),
    ("pickup_zone", T.StringType()), ("dropoff_zone", T.StringType()),
    ("pickup_ts", T.TimestampType()), ("dropoff_ts", T.TimestampType()),
    ("fare", MONEY), ("surge_multiplier", T.DoubleType()), ("distance_km", T.DoubleType()),
    ("ingest_ts", T.TimestampType()),
)

DRIVER_SCD2_SCHEMA = _struct(
    ("driver_id", T.StringType()), ("vehicle_type", T.StringType()), ("home_zone", T.StringType()),
    ("status", T.StringType()), ("record_hash", T.StringType()),
    ("effective_from", T.DateType()), ("effective_to", T.DateType()), ("is_current", T.BooleanType()),
)

CURATED_RIDES_SCHEMA = T.StructType(
    list(UNIFIED_SCHEMA.fields)
    + list(_struct(
        ("trip_minutes", T.DoubleType()), ("revenue_per_km", T.DoubleType()), ("surge_flag", T.BooleanType()),
        ("weekday", T.StringType()), ("hour_bucket", T.StringType()),
        ("pickup_zone_name", T.StringType()), ("pickup_borough", T.StringType()),
        ("dropoff_zone_name", T.StringType()), ("dropoff_borough", T.StringType()), ("zone_match", T.StringType()),
        ("driver_vehicle_type", T.StringType()), ("driver_home_zone", T.StringType()),
        ("driver_status", T.StringType()), ("driver_version_from", T.DateType()), ("driver_match", T.StringType()),
        ("processing_date", T.DateType()), ("run_id", T.StringType()),
        ("pickup_date", T.DateType()),  # partition column, last
    ).fields)
)

REJECTED_SCHEMA = T.StructType(
    list(UNIFIED_SCHEMA.fields)
    + list(_struct(
        ("reject_reason", T.StringType()), ("processing_date", T.DateType()), ("run_id", T.StringType()),
        ("event_date", T.DateType()),  # partition column, last
    ).fields)
)

_BQ_TYPES = {"string": "STRING", "timestamp": "TIMESTAMP", "date": "DATE", "double": "FLOAT64",
             "boolean": "BOOL", "bigint": "INT64"}


def bigquery_schema(schema: T.StructType) -> list[dict]:
    """BigQuery column list for a Parquet output schema (used by the warehouse layer)."""
    out = []
    for field in schema.fields:
        simple = field.dataType.simpleString()
        bq_type = "NUMERIC" if simple.startswith("decimal") else _BQ_TYPES[simple]
        out.append({"name": field.name, "type": bq_type, "mode": "NULLABLE"})
    return out


def signature(df_or_schema) -> list[tuple[str, str]]:
    schema = df_or_schema.schema if isinstance(df_or_schema, DataFrame) else df_or_schema
    return [(f.name, f.dataType.simpleString()) for f in schema.fields]


class SchemaMismatchError(ValueError):
    """Input columns are missing or have incompatible types. Fix the source; the job does not guess."""


_INTEGRAL = (T.ByteType, T.ShortType, T.IntegerType, T.LongType)
_NUMERIC = _INTEGRAL + (T.FloatType, T.DoubleType, T.DecimalType)


def _compatible(actual: T.DataType, expected: T.DataType) -> bool:
    if actual == expected:
        return True
    if isinstance(expected, T.LongType):
        return isinstance(actual, _INTEGRAL)
    if isinstance(expected, (T.DoubleType, T.DecimalType)):
        return isinstance(actual, _NUMERIC)
    return False


def conform(df: DataFrame, expected: T.StructType, name: str) -> DataFrame:
    """Validate `df` against `expected` and return exactly the expected columns with expected types."""
    actual = {f.name: f.dataType for f in df.schema.fields}
    missing = [f.name for f in expected.fields if f.name not in actual]
    wrong = [
        f"{f.name}: found {actual[f.name].simpleString()}, expected {f.dataType.simpleString()}"
        for f in expected.fields
        if f.name in actual and not _compatible(actual[f.name], f.dataType)
    ]
    if missing or wrong:
        raise SchemaMismatchError(
            f"{name} does not match the expected schema. Missing columns: {missing or 'none'}. "
            f"Incompatible types: {wrong or 'none'}."
        )
    return df.select([F.col(f.name).cast(f.dataType).alias(f.name) for f in expected.fields])
