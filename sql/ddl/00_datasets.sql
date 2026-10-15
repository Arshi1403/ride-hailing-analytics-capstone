-- Purpose: create the warehouse datasets. Safe to rerun (IF NOT EXISTS); never drops or alters existing datasets.
-- Params: none
-- Cost: metadata only. Needs approval before the first run because it creates resources.
-- Run: bq --project_id=<project> --location=asia-south1 query --use_legacy_sql=false < sql/ddl/00_datasets.sql
-- Location: edit the literal below if the project uses a different BigQuery location (OPTIONS cannot take parameters).

CREATE SCHEMA IF NOT EXISTS raw_ds
OPTIONS (location = 'asia-south1', description = 'Raw layer: append-only copies of source data. Not used by dashboards.');

CREATE SCHEMA IF NOT EXISTS stg_ds
OPTIONS (location = 'asia-south1', description = 'Staging: Parquet output of the Spark job, truncated and reloaded on every run.');

CREATE SCHEMA IF NOT EXISTS curated_ds
OPTIONS (location = 'asia-south1', description = 'Curated layer: validated, deduplicated, enriched rides and the SCD2 driver dimension.');

CREATE SCHEMA IF NOT EXISTS mart_ds
OPTIONS (location = 'asia-south1', description = 'Dashboard-facing views. Read curated and ML data only, never raw.');

CREATE SCHEMA IF NOT EXISTS ml_ds
OPTIONS (location = 'asia-south1', description = 'BQML models, forecast training series and forecast results.');

CREATE SCHEMA IF NOT EXISTS ops_ds
OPTIONS (location = 'asia-south1', description = 'Pipeline operations: data-quality results.');
