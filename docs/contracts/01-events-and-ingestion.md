# Contract 01: Events and Ingestion (v0.1.0, draft)

Affected agents: ingestion, spark, bigquery, testing.

## Pub/Sub message
Body: UTF-8 JSON, one event per message. Attributes: `event_type`, `event_version`. Ordering is not assumed. Delivery is at-least-once, so consumers dedupe by `event_id`.

## Ride event (`event_type` in `ride_requested`, `driver_assigned`, `ride_started`, `ride_completed`, `ride_cancelled`)
| Field | Type | Required | Rule |
|---|---|---|---|
| event_id | STRING (UUID) | yes | unique |
| event_type | STRING | yes | one of the 5 values |
| event_version | STRING | yes | `1.0` |
| event_ts | TIMESTAMP (ISO-8601 UTC, `Z`) | yes | parseable |
| ride_id | STRING | yes | non-empty |
| rider_id | STRING | yes | non-empty |
| driver_id | STRING | from `driver_assigned` on | non-empty |
| pickup_zone_id | STRING | yes | non-empty |
| dropoff_zone_id | STRING | no | |
| pickup_lat, pickup_lng | FLOAT64 | yes | -90..90, -180..180 |
| dropoff_lat, dropoff_lng | FLOAT64 | no | same ranges |
| fare_amount | NUMERIC | on `ride_completed` | >= 0 |
| currency | STRING | with fare | `INR` |
| distance_km | FLOAT64 | on `ride_completed` | >= 0 |
| payment_method | STRING | no | `CARD`, `CASH`, `WALLET`, `UPI` |
| vehicle_type | STRING | yes | `AUTO`, `MINI`, `SEDAN`, `SUV`, `BIKE` |
| surge_multiplier | FLOAT64 | no | default 1.0 |
| cancellation_reason | STRING | on `ride_cancelled` | |

## Driver profile event (`event_type` = `driver_profile_updated`)
`event_id`, `event_type`, `event_version`, `event_ts`, `driver_id`, `city`, `vehicle_type`, `status` (`ACTIVE`, `INACTIVE`, `SUSPENDED`), `home_zone_id`, `rating` (FLOAT64, 0..5). All required.

## Dataflow behaviour
- Parse JSON, check required fields, enum values, timestamp parse, numeric ranges (structural only).
- Valid ride events -> `rh_raw.ride_events_raw`; valid driver events -> `rh_raw.driver_events_raw`; add `pubsub_message_id`, `pubsub_publish_ts`, `ingest_ts`.
- Invalid -> `rh_raw.rejected_events(raw_payload STRING, error_code STRING, error_detail STRING, source STRING, ingest_ts TIMESTAMP)`. Nothing is dropped silently.
- Subscription dead-letter topic `PUBSUB_DLQ_TOPIC` after 5 delivery attempts (set by Terraform).

## Historical CSV (header row required, UTF-8, `,` delimiter, timestamps ISO-8601 UTC)
- `trips`: `ride_id, rider_id, driver_id, pickup_zone_id, dropoff_zone_id, requested_ts, started_ts, completed_ts, status, fare_amount, currency, distance_km, payment_method, vehicle_type, surge_multiplier, cancellation_reason`. `status` in `COMPLETED`, `CANCELLED`.
- `drivers`: `driver_id, city, vehicle_type, status, home_zone_id, rating, effective_ts`.
- `zones.csv`: `zone_id, zone_name, city, centroid_lat, centroid_lng`.
- Loader adds `source_file`, `batch_id`, `ingest_ts`. Load is idempotent per `batch_date` (marker object in `_loaded/`; reload replaces that batch only).

## Producer
CLI `python -m ingestion.producer`: `--dry-run` (default true, prints/saves JSONL), `--max-events`, `--seed`, `--invalid-rate` (default 0.02, injects bad events for DQ demos), `--topic`. Real publish needs `--no-dry-run` and user approval. Max default 100 events.
