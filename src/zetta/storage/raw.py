from __future__ import annotations

import gzip
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class RawRecord:
    source: str
    entity: str
    collected_at: str
    request_url: str
    payload: Any


class RawJsonlWriter:
    def __init__(self, root: Path) -> None:
        self.root = root

    def write(self, *, source: str, entity: str, request_url: str, payload: Any) -> Path:
        now = datetime.now(UTC)
        partition = self.root / f"source={source}" / f"entity={entity}" / f"dt={now.date()}"
        partition.mkdir(parents=True, exist_ok=True)
        output_path = partition / f"{now.strftime('%H%M%S%f')}.jsonl.gz"
        record = RawRecord(
            source=source,
            entity=entity,
            collected_at=now.isoformat(),
            request_url=request_url,
            payload=payload,
        )
        tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
        with gzip.open(tmp_path, "wt", encoding="utf-8") as handle:
            handle.write(json.dumps(asdict(record), ensure_ascii=False, separators=(",", ":")))
            handle.write("\n")
        tmp_path.replace(output_path)
        return output_path
