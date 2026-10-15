-- Purpose: idempotent SCD2 upsert of the driver dimension from the Spark full-history state (one snapshot date).
-- Params: @run_id STRING, @snapshot_date DATE (the snapshot the Spark run applied), @business_tz STRING (same value as the Spark --timezone, e.g. 'UTC')
-- Cost: dimension is small. Needs approval to run.
-- Inputs: stg_ds.dim_driver_scd2_stg = the latest snapshot_date partition of Spark's dim_driver_scd2 output (full history).
-- How history is preserved and changes are handled:
--   * Key = (driver_id, effective_start_ts). Spark never rewrites a closed version, so older versions match and stay unchanged.
--   * Changed attribute: Spark closes the old version (effective_to = snapshot date, is_current = false) and adds a new
--     current version. The MATCHED branch applies the close, the NOT MATCHED branch inserts the new version.
--   * New driver: inserted as a current version. Unchanged driver: no update (the update condition is false).
--   * Rerun of the same snapshot_date: no change if the content is identical. If Spark rewound and corrected that day,
--     versions that start on or after the snapshot date and are no longer in the source are deleted (the only DELETE).
--   * Versions starting before the snapshot date are never deleted. Drivers absent from staging are left alone.
-- Failure guard: the ASSERTs stop the script before any change on empty/mixed staging, a second current record,
--   duplicate versions, or a snapshot older than the history already stored.

ASSERT (SELECT COUNT(*) FROM stg_ds.dim_driver_scd2_stg) > 0
  AS 'stg_ds.dim_driver_scd2_stg is empty: load the latest Spark snapshot_date partition first';
ASSERT (SELECT COUNT(DISTINCT snapshot_date) FROM stg_ds.dim_driver_scd2_stg) = 1
      AND (SELECT MAX(snapshot_date) FROM stg_ds.dim_driver_scd2_stg) = @snapshot_date
  AS 'staging must contain exactly one snapshot_date and it must equal @snapshot_date';
ASSERT NOT EXISTS (SELECT 1 FROM stg_ds.dim_driver_scd2_stg GROUP BY driver_id HAVING COUNTIF(is_current) != 1)
  AS 'every driver in staging must have exactly one current version';
ASSERT NOT EXISTS (SELECT 1 FROM stg_ds.dim_driver_scd2_stg GROUP BY driver_id, effective_from HAVING COUNT(*) > 1)
  AS 'duplicate (driver_id, effective_from) in staging';
ASSERT NOT EXISTS (
  SELECT 1 FROM curated_ds.dim_driver_scd2 WHERE effective_start_ts > TIMESTAMP(@snapshot_date, @business_tz)
) AS 'dim_driver_scd2 already holds versions newer than @snapshot_date: only the latest snapshot can be (re)applied';

MERGE curated_ds.dim_driver_scd2 AS T
USING (
  SELECT
    driver_id,
    vehicle_type,
    home_zone,
    status,
    record_hash,
    TIMESTAMP(effective_from, @business_tz) AS effective_start_ts,
    IF(is_current, TIMESTAMP '9999-12-31 00:00:00+00', TIMESTAMP(effective_to, @business_tz)) AS effective_end_ts,
    is_current
  FROM stg_ds.dim_driver_scd2_stg
) AS S
ON T.driver_id = S.driver_id AND T.effective_start_ts = S.effective_start_ts
WHEN MATCHED AND (
     T.effective_end_ts != S.effective_end_ts
  OR T.is_current != S.is_current
  OR T.record_hash != S.record_hash
  OR T.vehicle_type IS DISTINCT FROM S.vehicle_type
  OR T.home_zone IS DISTINCT FROM S.home_zone
  OR T.status IS DISTINCT FROM S.status
) THEN UPDATE SET
  vehicle_type = S.vehicle_type,
  home_zone = S.home_zone,
  status = S.status,
  record_hash = S.record_hash,
  effective_end_ts = S.effective_end_ts,
  is_current = S.is_current,
  run_id = @run_id
WHEN NOT MATCHED BY TARGET THEN INSERT (
  driver_id, vehicle_type, home_zone, status, record_hash, effective_start_ts, effective_end_ts, is_current, loaded_ts, run_id
) VALUES (
  S.driver_id, S.vehicle_type, S.home_zone, S.status, S.record_hash, S.effective_start_ts, S.effective_end_ts,
  S.is_current, CURRENT_TIMESTAMP(), @run_id
)
WHEN NOT MATCHED BY SOURCE AND T.effective_start_ts >= TIMESTAMP(@snapshot_date, @business_tz) THEN DELETE;
