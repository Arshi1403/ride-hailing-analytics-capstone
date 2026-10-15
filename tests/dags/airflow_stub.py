"""A minimal stand-in for Apache Airflow so the DAG file can be imported and its graph inspected without Airflow installed.

It models only what ride_hailing_daily.py uses: DAG as a context manager, operators that register themselves and support
`>>`, default_args inheritance for retry settings, Param, TriggerRule, exceptions and pendulum.datetime. It is NOT Airflow:
passing these tests proves the graph and arguments we wrote, not that Airflow accepts them (see tests/dags/test_dags_airflow_import.py).
"""
import sys
import types
from datetime import datetime

_current = []


class AirflowFailException(Exception):
    pass


class AirflowSkipException(Exception):
    pass


class Param:
    def __init__(self, default=None, **schema):
        self.value, self.schema = default, schema


class TriggerRule:
    ALL_SUCCESS = "all_success"
    NONE_FAILED = "none_failed"
    NONE_FAILED_MIN_ONE_SUCCESS = "none_failed_min_one_success"


class DAG:
    def __init__(self, dag_id, **kwargs):
        self.dag_id = dag_id
        self.kwargs = kwargs
        self.default_args = kwargs.get("default_args", {})
        self.task_dict = {}

    def __getattr__(self, name):
        try:
            return self.__dict__["kwargs"][name]
        except KeyError:
            raise AttributeError(name) from None

    def __enter__(self):
        _current.append(self)
        return self

    def __exit__(self, *exc):
        _current.pop()

    @property
    def tasks(self):
        return list(self.task_dict.values())

    @property
    def task_ids(self):
        return list(self.task_dict)

    def get_task(self, task_id):
        return self.task_dict[task_id]


class BaseOperator:
    def __init__(self, task_id, **kwargs):
        self.task_id = task_id
        self.kwargs = kwargs
        self.dag = _current[-1]
        self.upstream_task_ids, self.downstream_task_ids = set(), set()
        if task_id in self.dag.task_dict:
            raise ValueError(f"duplicate task id {task_id}")
        self.dag.task_dict[task_id] = self

    def _resolve(self, name, default=None):
        if name in self.kwargs:
            return self.kwargs[name]
        return self.dag.default_args.get(name, default)

    retries = property(lambda self: self._resolve("retries", 0))
    retry_exponential_backoff = property(lambda self: self._resolve("retry_exponential_backoff", False))
    execution_timeout = property(lambda self: self._resolve("execution_timeout"))
    sla = property(lambda self: self._resolve("sla"))
    trigger_rule = property(lambda self: self.kwargs.get("trigger_rule", TriggerRule.ALL_SUCCESS))

    def _link(self, others):
        for other in others if isinstance(others, (list, tuple)) else [others]:
            self.downstream_task_ids.add(other.task_id)
            other.upstream_task_ids.add(self.task_id)

    def __rshift__(self, other):
        self._link(other)
        return other

    def __rrshift__(self, others):
        for item in others:
            item._link(self)
        return self


def _operator(name):
    return type(name, (BaseOperator,), {})


def install():
    """Register the stub modules in sys.modules. Returns a function that removes them again."""
    names = {
        "airflow": {"DAG": DAG},
        "airflow.models": {},
        "airflow.models.param": {"Param": Param},
        "airflow.exceptions": {"AirflowFailException": AirflowFailException, "AirflowSkipException": AirflowSkipException},
        "airflow.operators": {},
        "airflow.operators.empty": {"EmptyOperator": _operator("EmptyOperator")},
        "airflow.operators.python": {"PythonOperator": _operator("PythonOperator"), "BranchPythonOperator": _operator("BranchPythonOperator")},
        "airflow.providers": {},
        "airflow.providers.google": {},
        "airflow.providers.google.cloud": {},
        "airflow.providers.google.cloud.operators": {},
        "airflow.providers.google.cloud.operators.bigquery": {"BigQueryInsertJobOperator": _operator("BigQueryInsertJobOperator")},
        "airflow.providers.google.cloud.operators.dataproc": {"DataprocCreateBatchOperator": _operator("DataprocCreateBatchOperator")},
        "airflow.providers.google.cloud.sensors": {},
        "airflow.providers.google.cloud.sensors.gcs": {"GCSObjectExistenceSensor": _operator("GCSObjectExistenceSensor")},
        "airflow.providers.google.cloud.transfers": {},
        "airflow.providers.google.cloud.transfers.gcs_to_bigquery": {"GCSToBigQueryOperator": _operator("GCSToBigQueryOperator")},
        "airflow.utils": {},
        "airflow.utils.trigger_rule": {"TriggerRule": TriggerRule},
        "pendulum": {"datetime": lambda y, m, d, tz="UTC": datetime(y, m, d)},
    }
    added = []
    for name, attrs in names.items():
        if name not in sys.modules:
            module = types.ModuleType(name)
            module.__dict__.update(attrs)
            module.__path__ = []  # allow submodule imports
            sys.modules[name] = module
            added.append(name)

    def remove():
        for name in added:
            sys.modules.pop(name, None)

    return remove
