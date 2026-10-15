-- Purpose: idempotent load of rejected rides for one run's window (replace the window, then insert).
-- Params: @run_id STRING, @window_start DATE, @window_end DATE
-- Cost: scans only the event_date partitions in the window. Needs approval to run.
-- Inputs: stg_ds.rejected_rides_stg holds this run's rejects. It may be empty (no rejects), but it must have been
--   reloaded for this run: stale rows from another run_id stop the script.
-- Rerun safety: the delete and insert run in one transaction, so a rerun replaces the window instead of duplicating it.

ASSERT (SELECT COUNTIF(run_id != @run_id OR run_id IS NULL) FROM stg_ds.rejected_rides_stg) = 0
  AS 'stg_ds.rejected_rides_stg contains rows from a different run_id';
ASSERT (SELECT COUNTIF(event_date IS NULL OR event_date NOT BETWEEN @window_start AND @window_end) FROM stg_ds.rejected_rides_stg) = 0
  AS 'stg_ds.rejected_rides_stg has event_date outside the window';

BEGIN TRANSACTION;

DELETE FROM curated_ds.rejected_rides
WHERE event_date BETWEEN @window_start AND @window_end;

INSERT INTO curated_ds.rejected_rides (
  ride_id, source, source_file, driver_id, rider_id, pickup_zone, dropoff_zone, pickup_ts, dropoff_ts, fare,
  surge_multiplier, distance_km, ingest_ts, reject_reason, processing_date, run_id, event_date
)
SELECT
  ride_id, source, source_file, driver_id, rider_id, pickup_zone, dropoff_zone, pickup_ts, dropoff_ts, fare,
  surge_multiplier, distance_km, ingest_ts, reject_reason, processing_date, run_id, event_date
FROM stg_ds.rejected_rides_stg;

COMMIT TRANSACTION;
