from ingestion.config import load_config
from ingestion.dataflow import build_gcloud_command, build_parameters, main

ENV = {
    "DATAFLOW_TEMP_LOCATION": "gs://my-bucket/dataflow/temp",
    "DATAFLOW_UDF_GCS_PATH": "gs://my-bucket/dataflow/udf/ride_event_udf.js",
    "DATAFLOW_SERVICE_ACCOUNT": "sa-dataflow@labs-507309.iam.gserviceaccount.com",
}


def test_parameters_use_subscription_not_topic():
    params = build_parameters(load_config(ENV))
    assert params["inputSubscription"] == "projects/labs-507309/subscriptions/rides-events-sub"
    assert "inputTopic" not in params
    assert params["outputTableSpec"] == "labs-507309:raw_ds.rides_stream"
    assert params["outputDeadletterTable"] == "labs-507309:raw_ds.rides_stream_error_records"
    assert params["javascriptTextTransformFunctionName"] == "process"


def test_command_is_complete_and_only_a_list_of_strings():
    command = build_gcloud_command(load_config(ENV))
    assert command[:4] == ["gcloud", "dataflow", "jobs", "run"]
    joined = " ".join(command)
    for expected in ("--region=asia-south1", "--project=labs-507309", "--max-workers=2",
                     "--service-account-email=sa-dataflow@labs-507309.iam.gserviceaccount.com",
                     "--gcs-location=gs://dataflow-templates-asia-south1/latest/PubSub_Subscription_to_BigQuery",
                     "--staging-location=gs://my-bucket/dataflow/temp"):
        assert expected in joined


def test_main_prints_command(monkeypatch, capsys):
    for key, value in ENV.items():
        monkeypatch.setenv(key, value)
    assert main([]) == 0
    assert capsys.readouterr().out.startswith("gcloud dataflow jobs run")


def test_main_reports_missing_config(monkeypatch, capsys):
    for key in ENV:
        monkeypatch.delenv(key, raising=False)
    assert main([]) == 2
    assert "DATAFLOW_UDF_GCS_PATH" in capsys.readouterr().err
