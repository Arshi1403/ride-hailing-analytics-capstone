"""SparkSession factory with the settings the job relies on."""
from __future__ import annotations

from pyspark.sql import SparkSession

_SQL_CONF = {
    "spark.sql.session.timeZone": "UTC",  # derive.py assumes this
    "spark.sql.sources.partitionOverwriteMode": "dynamic",  # reruns replace only the partitions they write
    "spark.sql.parquet.outputTimestampType": "TIMESTAMP_MICROS",
    "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
}


def build_session(app_name: str = "ride-processing", master: str | None = None) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    spark = builder.getOrCreate()
    for key, value in _SQL_CONF.items():
        spark.conf.set(key, value)
    return spark
