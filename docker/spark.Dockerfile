# The batch path's runtime.
#
# Built on python:3.12 with a JDK added, rather than on an official Spark image
# with Python added. The Spark images ship whatever Python their base happened
# to have — 3.8 through 3.11 depending on the tag — and this codebase needs 3.12
# for `StrEnum` alone. PySpark bundles its own Spark distribution, so the only
# thing actually missing from a Python image is a JVM.
#
# JRE 17 specifically. PySpark 3.5 supports 8, 11 and 17; the JDK on the machine
# this was developed on is 23, which it does not, and that is the whole reason
# the batch job runs in a container here.
#
# Pinned to bookworm rather than plain `-slim`. Debian 13 dropped
# openjdk-17-jre-headless from its main archive, so the unpinned tag started
# failing to build the moment the Python image moved base — the kind of break
# that arrives without anything in this repo changing.
FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements-spark.txt requirements-dev.txt /tmp/reqs/
RUN pip install --no-cache-dir \
        -r /tmp/reqs/requirements-spark.txt \
        -r /tmp/reqs/requirements-dev.txt

# The repo is mounted rather than copied, so an edit is runnable without a
# rebuild. Only the dependency layers above are baked in.
WORKDIR /app
ENV PYTHONPATH=/app \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3
