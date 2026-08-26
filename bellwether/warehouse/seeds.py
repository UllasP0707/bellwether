"""Exporting the signal catalog as a dbt seed.

The marts need each signal's weight and category to compute anything useful.
Typing those into SQL would put a second copy of the scoring model in the
warehouse, and it would go stale the first time somebody rebalanced a weight —
the same failure the stream and batch paths are structured to avoid, just
one layer further out where no parity test would see it.

So the seed is generated from `bellwether.scoring.catalog` and checked into the
repo, and `tests/test_warehouse.py` regenerates it and fails if the file on disk
has drifted. A committed artefact that a test keeps honest: reviewable in a
diff, and impossible to forget to refresh.
"""

from __future__ import annotations

import csv
import io
from pathlib import Path

from bellwether.scoring.catalog import CATALOG

SEED = Path(__file__).resolve().parents[2] / "transform" / "seeds" / "signal_catalog.csv"

HEADER = ("signal", "category", "weight", "half_life_days", "is_mitigating", "description")


def render() -> str:
    """The seed's exact contents, as the catalog says they should be."""
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(HEADER)
    for signal in sorted(CATALOG, key=lambda s: s.value):
        spec = CATALOG[signal]
        writer.writerow(
            (
                spec.signal.value,
                spec.category.value,
                spec.weight,
                spec.half_life_days,
                str(spec.is_mitigating).lower(),
                # First sentence only. The rest of a catalog description argues
                # about weighting, which belongs in the source and not in a
                # warehouse column somebody might put on a dashboard.
                spec.description.split(".")[0].strip(),
            )
        )
    return buffer.getvalue()


def write(path: Path | None = None) -> Path:
    target = path or SEED
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(render())
    return target


def is_current(path: Path | None = None) -> bool:
    target = path or SEED
    return target.exists() and target.read_text() == render()
