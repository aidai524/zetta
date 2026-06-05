from __future__ import annotations

import gzip
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any


def iter_raw_records(root: Path, *, source: str | None = None, entity: str | None = None) -> Iterator[dict[str, Any]]:
    pattern = "*.jsonl.gz"
    for path in sorted(root.rglob(pattern)):
        path_text = str(path)
        if source and f"source={source}" not in path_text:
            continue
        if entity and f"entity={entity}" not in path_text:
            continue
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    record = json.loads(line)
                    record["_raw_path"] = str(path)
                    yield record

