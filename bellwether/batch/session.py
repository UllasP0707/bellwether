"""Spark session construction.

Isolated here so the rest of the batch package imports nothing from PySpark at
module scope, which is what lets `bellwether.batch.parity` be importable — and
its non-Spark half runnable — on a machine with no JVM.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession


def spark_session(
    app: str = "bellwether", master: str = "local[*]", extra: dict[str, str] | None = None
) -> SparkSession:
    """A local session, configured for determinism over throughput.

    `spark.sql.shuffle.partitions` is dropped from its default of 200 because
    these jobs run over one company's events, and 200 partitions of a few
    thousand rows costs more in task overhead than the parallelism buys.

    The session is also pinned to UTC. Spark reads the driver's timezone by
    default, so a job producing daily rollups would silently bucket events by
    whatever the machine happened to be set to — and the stream path, which
    windows in UTC, would disagree with it for anyone west of Greenwich.
    """
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
    )
    for key, value in (extra or {}).items():
        builder = builder.config(key, value)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session


def s3a_options(endpoint: str, access_key: str, secret_key: str) -> dict[str, str]:
    """Config for reading the MinIO-backed lake over s3a://.

    Not applied by default: the local lake is a directory, and requiring the
    Hadoop AWS jars to run a job over local files would make the batch path
    harder to try than it needs to be.
    """
    return {
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.access.key": access_key,
        "spark.hadoop.fs.s3a.secret.key": secret_key,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    }


def stop(session: Any) -> None:
    session.stop()
