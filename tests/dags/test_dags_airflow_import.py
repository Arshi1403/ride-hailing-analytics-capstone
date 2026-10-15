"""Runs only where Apache Airflow (and the Google provider) is installed, such as a Composer-matching image.
It uses the real DagBag, so it proves what the stub tests cannot: that Airflow accepts the operators and arguments."""
import pytest

pytest.importorskip("airflow", reason="Airflow is not installed here: import against real Airflow is NOT verified")
pytest.importorskip("airflow.providers.google", reason="apache-airflow-providers-google is not installed")

from airflow.models import DagBag  # noqa: E402

from dag_loader import DAGS, ENV  # noqa: E402


@pytest.fixture(scope="module")
def dagbag(monkeypatch_module):
    for key, value in ENV.items():
        monkeypatch_module.setenv(key, value)
    return DagBag(dag_folder=str(DAGS), include_examples=False)


@pytest.fixture(scope="module")
def monkeypatch_module():
    from _pytest.monkeypatch import MonkeyPatch

    patch = MonkeyPatch()
    yield patch
    patch.undo()


def test_dag_imports_without_errors(dagbag):
    assert dagbag.import_errors == {}
    assert "ride_hailing_daily" in dagbag.dags


def test_graph_has_no_cycles_and_expected_size(dagbag):
    dag = dagbag.dags["ride_hailing_daily"]
    dag.test_cycle()
    assert len(dag.tasks) == 22
