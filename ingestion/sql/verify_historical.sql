-- Replace raw_ds.rides_history with your configured dataset/table before running. Read-only.

-- 1. Rows per source file (the rerun key); rerunning a file must not change its count
SELECT source_file, COUNT(*) AS rows_loaded, MAX(load_id) AS last_load_id, MAX(ingest_ts) AS last_ingest
FROM `labs-507309.raw_ds.rides_history`
GROUP BY source_file ORDER BY source_file;

-- 2. Year guard: must be exactly one row, year 2018
SELECT EXTRACT(YEAR FROM tpep_pickup_datetime) AS pickup_year, COUNT(*) AS n
FROM `labs-507309.raw_ds.rides_history`
GROUP BY pickup_year;

-- 3. Sanity: nulls and ranges
SELECT COUNTIF(tpep_pickup_datetime IS NULL) AS null_pickup,
       COUNTIF(tpep_dropoff_datetime < tpep_pickup_datetime) AS dropoff_before_pickup,
       COUNTIF(fare_amount < 0) AS negative_fare,
       COUNTIF(PULocationID IS NULL) AS null_pickup_zone
FROM `labs-507309.raw_ds.rides_history`;

-- 4. Leftover staging tables from failed runs (safe to delete; they expire after 24 h)
SELECT table_name FROM `labs-507309.raw_ds.INFORMATION_SCHEMA.TABLES`
WHERE table_name LIKE 'rides_history_stg_%';
