"""Load the DAG file with a valid environment, using real Airflow when installed and the stub otherwise."""
import importlib
import importlib.util
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DAGS = ROOT / "dags"

ENV = {
    "GCP_PROJECT_ID": "labs-507309",
    "GCP_REGION": "asia-south1",
    "RH_LANDING_BUCKET": "rh-landing-test",
    "RH_CURATED_BUCKET": "rh-curated-test",
    "RH_ARTIFACTS_BUCKET": "rh-artifacts-test",
    "RH_SPARK_SERVICE_ACCOUNT": "sa-spark@labs-507309.iam.gserviceaccount.com",
}

if str(DAGS) not in sys.path:
    sys.path.insert(0, str(DAGS))


def real_airflow_available() -> bool:
    return importlib.util.find_spec("airflow") is not None


def arg(task, name):
    """Constructor argument of an operator: the stub keeps them in .kwargs, real Airflow exposes attributes."""
    return task.kwargs[name] if hasattr(task, "kwargs") else getattr(task, name)


def load_dag(env=None, **extra_env):
    """Import dags/ride_hailing_daily.py fresh with the given environment. Returns the DAG object."""
    from airflow_stub import install

    merged = dict(ENV if env is None else env)
    merged.update(extra_env)
    saved = {k: os.environ.get(k) for k in list(merged) + [k for k in os.environ if k.startswith("RH_")]}
    for key in [k for k in os.environ if k.startswith("RH_")]:
        os.environ.pop(key)
    os.environ.update(merged)
    remove = (lambda: None) if real_airflow_available() else install()
    try:
        for name in [n for n in sys.modules if n == "common" or n.startswith("common.") or n == "ride_hailing_daily"]:
            sys.modules.pop(name)
        module = importlib.import_module("ride_hailing_daily")
        return module.dag
    finally:
        remove()
        for key in [k for k in os.environ if k.startswith("RH_")]:
            os.environ.pop(key)
        for key, value in saved.items():
            if value is not None:
                os.environ[key] = value
        for name in [n for n in sys.modules if n == "ride_hailing_daily"]:
            sys.modules.pop(name)
