# Producer

Publishes mock ride events to Pub/Sub topic `rides-events`. Default is a **dry run**: nothing is sent unless you pass `--publish`.

```powershell
pip install -r producer/requirements.txt          # only needed for --publish
python -m producer --help
```

| Goal | Command |
|---|---|
| Dry run, 10 events | `python -m producer --count 10` |
| Dry run, save all events | `python -m producer --count 1000 --output events.jsonl` |
| Tiny real test (max 50 events) | `python -m producer --count 10 --duration-seconds 10 --publish` |
| Dead-letter test | `python -m producer --count 10 --duration-seconds 10 --inject-invalid 2 --publish` |
| Full run: 1,000 events over 10 minutes | `python -m producer --publish --confirm-full-run` (approval required) |

Options also read environment variables (`producer/config.example.env`): `GCP_PROJECT_ID`, `PUBSUB_TOPIC`, `PRODUCER_COUNT`, `PRODUCER_DURATION_SECONDS`, `PRODUCER_SEED`. Command-line flags win.

Safety limits: publishing more than 50 events needs `--confirm-full-run`; at most 5000 events and 50 events/second; at most 5 invalid events when publishing. Same `--seed` and `--start-time` give identical events. Events are paced evenly: event *i* is sent at `i * duration / count` seconds. Credentials come from Application Default Credentials (`gcloud auth application-default login`).

Exit codes: 0 success, 1 publish/runtime failure, 2 configuration error. Logs are JSON lines on stderr; the summary is JSON on stdout.

Schema, `ingest_ts`, Dataflow, IAM and verification: see [ingestion/README.md](../ingestion/README.md).
