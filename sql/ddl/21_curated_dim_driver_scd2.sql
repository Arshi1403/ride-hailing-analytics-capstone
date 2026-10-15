-- Purpose: curated SCD Type 2 driver dimension. One row per driver version; history is never deleted by normal runs.
-- Source: spark/ridespark/scd2.py output (dates), converted to timestamps by sql/merge/merge_dim_driver_scd2.sql.
-- Params: none
-- Cost: metadata only. Approval needed (creates a table).
-- Validity: effective_start_ts <= t < effective_end_ts. The current version ends at 9999-12-31 00:00:00 UTC and has is_current = TRUE.
-- effective_start_ts of a driver's first version is the first snapshot date that contained the driver, not the real start date.
-- Invariants (checked by sql/data_quality/dq_duplicates.sql): exactly one current row per driver, no overlaps, contiguous versions.

CREATE TABLE IF NOT EXISTS curated_ds.dim_driver_scd2 (
  driver_id STRING NOT NULL,
  vehicle_type STRING,
  home_zone STRING,
  status STRING,
  record_hash STRING NOT NULL OPTIONS (description = 'sha256 of the tracked attributes; computed by Spark'),
  effective_start_ts TIMESTAMP NOT NULL,
  effective_end_ts TIMESTAMP NOT NULL,
  is_current BOOL NOT NULL,
  loaded_ts TIMESTAMP NOT NULL OPTIONS (description = 'When this version row was first inserted'),
  run_id STRING NOT NULL OPTIONS (description = 'Run that last wrote this row')
)
CLUSTER BY driver_id
OPTIONS (description = 'SCD2 driver dimension. Written only by sql/merge/merge_dim_driver_scd2.sql.');
