"""Raw payload archival.

Every vendor payload is written down verbatim before anything parses it, and the
resulting URI travels on the event as `raw_ref`.

This is the cheapest insurance in the system. Connector parsing is the part most
likely to be quietly wrong — a field that means something different than you
assumed, a vendor changing an enum — and without the original payload the only
fix is to wait for the data to happen again. With it, the fix is a reprocess.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from bellwether.events.schema import Source


class RawArchive(Protocol):
    """Somewhere to put a vendor payload before parsing it."""

    def put(
        self, source: Source, source_event_id: str, occurred_at: datetime, payload: dict[str, Any]
    ) -> str:
        """Store the payload and return a reference to it."""
        ...


def _key(source: Source, source_event_id: str, occurred_at: datetime) -> str:
    """Partition by source and event date.

    Date-partitioned so retention can be enforced by deleting whole prefixes,
    which is the only deletion granularity that stays cheap at volume.
    """
    day = occurred_at.strftime("%Y-%m-%d")
    safe_id = source_event_id.replace("/", "_")
    return f"raw/source={source.value}/dt={day}/{safe_id}.json"


class NullArchive:
    """Discards payloads, returning the reference they would have had.

    For dry runs and for tests that care about the event, not the bytes.
    """

    def put(
        self, source: Source, source_event_id: str, occurred_at: datetime, payload: dict[str, Any]
    ) -> str:
        return f"null://{_key(source, source_event_id, occurred_at)}"


class FileArchive:
    """Writes payloads under a local directory, mirroring the S3 key layout.

    `root` is the bucket equivalent, so keys land at `<root>/raw/source=.../`.
    """

    def __init__(self, root: Path | str = "data") -> None:
        self.root = Path(root)
        self.written = 0

    def put(
        self, source: Source, source_event_id: str, occurred_at: datetime, payload: dict[str, Any]
    ) -> str:
        key = _key(source, source_event_id, occurred_at)
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
        self.written += 1
        return f"file://{path}"


class S3Archive:
    """Writes payloads to S3 (MinIO locally).

    One object per payload. That is the wrong shape for analytics — millions of
    tiny objects are slow and expensive to scan — but it is the right shape for
    a landing zone whose job is durability and point lookup by `raw_ref`.
    Compaction into Parquet is the batch layer's problem, and it can do it
    because the originals are all still here.
    """

    def __init__(
        self,
        bucket: str,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        region: str = "us-east-1",
    ) -> None:
        import boto3

        self.bucket = bucket
        self.written = 0
        self._s3 = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
        )

    def put(
        self, source: Source, source_event_id: str, occurred_at: datetime, payload: dict[str, Any]
    ) -> str:
        key = _key(source, source_event_id, occurred_at)
        self._s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(payload, separators=(",", ":")).encode(),
            ContentType="application/json",
        )
        self.written += 1
        return f"s3://{self.bucket}/{key}"
