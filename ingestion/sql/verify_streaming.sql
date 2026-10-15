-- Replace raw_ds.rides_stream with your configured dataset/table before running. Read-only.

-- 1. Rows arriving, newest first (expect ~1000 after the full run)
SELECT COUNT(*) AS rows_total, MIN(ingest_ts) AS first_ingest, MAX(ingest_ts) AS last_ingest
FROM `labs-507309.raw_ds.rides_stream`;

-- 2. Duplicates by ride_id (at-least-once delivery can duplicate; expect 0 for a clean run)
SELECT ride_id, COUNT(*) AS copies
FROM `labs-507309.raw_ds.rides_stream`
GROUP BY ride_id HAVING COUNT(*) > 1 LIMIT 20;

-- 3. ingest_ts is populated and not before pickup_ts by an absurd margin
SELECT COUNTIF(ingest_ts IS NULL) AS null_ingest_ts,
       APPROX_QUANTILES(TIMESTAMP_DIFF(ingest_ts, pickup_ts, SECOND), 5) AS lag_seconds_quantiles
FROM `labs-507309.raw_ds.rides_stream`;

-- 4. Value ranges (all counts must be 0)
SELECT COUNTIF(fare <= 0 OR fare > 1000) AS bad_fare,
       COUNTIF(surge_multiplier < 1 OR surge_multiplier > 5) AS bad_surge,
       COUNTIF(dropoff_ts < pickup_ts) AS bad_order
FROM `labs-507309.raw_ds.rides_stream`;

-- 5. Dead-letter table: rejected messages and why (expect only injected invalid events)
SELECT timestamp, errorMessage, payloadString
FROM `labs-507309.raw_ds.rides_stream_error_records`
ORDER BY timestamp DESC LIMIT 20;
