import pytest

from ingestion.config import ConfigError, IngestionConfig, load_config, require_for_dataflow
from ingestion.schemas import HIST_CSV_COLUMNS, STREAM_COLUMNS, dataset_ddl, hist_ddl, stream_ddl
from producer.schema import FIELDS

GOOD_DATAFLOW = {
    "DATAFLOW_TEMP_LOCATION": "gs://my-bucket/dataflow/temp",
    "DATAFLOW_UDF_GCS_PATH": "gs://my-bucket/dataflow/udf/ride_event_udf.js",
    "DATAFLOW_SERVICE_ACCOUNT": "sa-dataflow@labs-507309.iam.gserviceaccount.com",
}


def test_defaults():
    cfg = load_config({})
    assert (cfg.project_id, cfg.region, cfg.raw_dataset, cfg.stream_table) == (
        "labs-507309", "asia-south1", "raw_ds", "rides_stream")
    assert cfg.dlq_table == "rides_stream_error_records"
    assert cfg.template_path == "gs://dataflow-templates-asia-south1/latest/PubSub_Subscription_to_BigQuery"
    assert cfg.stream_table_spec == "labs-507309:raw_ds.rides_stream"
    assert cfg.fq_table("t") == "labs-507309.raw_ds.t"


def test_env_overrides_destination():
    cfg = load_config({"BQ_RAW_DATASET": "other_ds", "BQ_STREAM_TABLE": "events"})
    assert cfg.stream_table_spec == "labs-507309:other_ds.events"
    assert cfg.dlq_table == "events_error_records"


@pytest.mark.parametrize("env,fragment", [
    ({"GCP_PROJECT_ID": "Bad Project"}, "GCP_PROJECT_ID"),
    ({"BQ_RAW_DATASET": "raw-ds"}, "BQ_RAW_DATASET"),
    ({"BQ_STREAM_TABLE": "a; DROP TABLE x"}, "BQ_STREAM_TABLE"),
    ({"PUBSUB_SUBSCRIPTION": "projects/p/subscriptions/s"}, "PUBSUB_SUBSCRIPTION"),
    ({"BQ_HIST_TABLE": "rides_stream"}, "must differ"),
    ({"HIST_YEAR": "2019"}, "2018"),
    ({"HIST_YEAR": "abc"}, "integers"),
    ({"DATAFLOW_MAX_WORKERS": "99"}, "MAX_WORKERS"),
    ({"HIST_FILE_PATTERN": "("}, "HIST_FILE_PATTERN"),
])
def test_invalid_config_is_rejected(env, fragment):
    with pytest.raises(ConfigError, match=fragment):
        load_config(env)


def test_dataflow_requirements_listed():
    with pytest.raises(ConfigError) as info:
        require_for_dataflow(load_config({}))
    assert len(info.value.problems) == 3
    require_for_dataflow(load_config(GOOD_DATAFLOW))


def test_stream_schema_matches_event_schema_plus_ingest_ts():
    names = [n for n, _, _ in STREAM_COLUMNS]
    assert names[:-1] == list(FIELDS)
    assert names[-1] == "ingest_ts"


def test_stream_ddl_contents():
    ddl = stream_ddl(IngestionConfig())
    assert "`labs-507309.raw_ds.rides_stream`" in ddl
    assert "ingest_ts TIMESTAMP NOT NULL" in ddl and "fare NUMERIC NOT NULL" in ddl
    assert "PARTITION BY DATE(ingest_ts)" in ddl and "CLUSTER BY pickup_zone" in ddl
    assert "CREATE OR REPLACE" not in ddl and "IF NOT EXISTS" in ddl


def test_hist_ddl_contents():
    ddl = hist_ddl(IngestionConfig())
    for name, _ in HIST_CSV_COLUMNS:
        assert name in ddl
    assert "source_file STRING NOT NULL" in ddl and "2018" in ddl


def test_dataset_ddl_uses_region():
    assert 'location="asia-south1"' in dataset_ddl(IngestionConfig())
