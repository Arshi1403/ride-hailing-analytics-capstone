import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "spark"))


@pytest.fixture(scope="session")
def spark():
    import os

    from pyspark.sql import SparkSession

    from ridespark.session import build_session

    # Python workers must use this interpreter (the one with pyspark), not whatever `python` is first on PATH.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    SparkSession.builder.master("local[2]").appName("ridespark-tests") \
        .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "false").getOrCreate()
    session = build_session()  # reuses the session above and applies the job's settings
    yield session
    session.stop()


@pytest.fixture(scope="session")
def local_parquet_io(spark, tmp_path_factory):
    probe = str(tmp_path_factory.mktemp("probe") / "p")
    try:
        spark.range(1).write.parquet(probe)
        spark.read.parquet(probe).count()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"local Parquet I/O unavailable ({type(exc).__name__}); on Windows install winutils/hadoop.dll and set HADOOP_HOME")
