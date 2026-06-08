from __future__ import annotations

import os
import atexit
import gzip
import json
import threading
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TextIO


@dataclass(frozen=True)
class RawRecord:
    source: str
    entity: str
    collected_at: str
    request_url: str
    payload: Any


class RawJsonlWriter:
    def __init__(
        self,
        root: Path,
        *,
        chunk_records: int = 1,
        chunk_seconds: float = 60.0,
    ) -> None:
        if chunk_records <= 0:
            raise ValueError("chunk_records must be positive")
        if chunk_seconds <= 0:
            raise ValueError("chunk_seconds must be positive")
        self.root = root
        self.chunk_records = chunk_records
        self.chunk_seconds = chunk_seconds
        self._lock = threading.Lock()
        self._chunks: dict[tuple[str, str, str], ChunkState] = {}
        atexit.register(self.flush)

    def write(self, *, source: str, entity: str, request_url: str, payload: Any) -> Path:
        now = datetime.now(UTC)
        record = RawRecord(
            source=source,
            entity=entity,
            collected_at=now.isoformat(),
            request_url=request_url,
            payload=payload,
        )
        if self.chunk_records == 1:
            return self._write_single(source=source, entity=entity, now=now, record=record)
        return self._write_chunk(source=source, entity=entity, now=now, record=record)

    def _write_single(
        self,
        *,
        source: str,
        entity: str,
        now: datetime,
        record: RawRecord,
    ) -> Path:
        partition = self._partition(source, entity, now)
        output_path = partition / f"{now.strftime('%H%M%S%f')}.jsonl.gz"
        tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
        with gzip.open(tmp_path, "wt", encoding="utf-8", compresslevel=1) as handle:
            write_record(handle, record)
        tmp_path.replace(output_path)
        return output_path

    def _write_chunk(
        self,
        *,
        source: str,
        entity: str,
        now: datetime,
        record: RawRecord,
    ) -> Path:
        partition = self._partition(source, entity, now)
        key = (source, entity, str(now.date()))
        with self._lock:
            state = self._chunks.get(key)
            if state is not None and self._should_finalize(state):
                self._finalize(state)
                self._chunks.pop(key, None)
                state = None
            if state is None:
                state = self._new_chunk_state(partition, now)
                self._chunks[key] = state
            write_record(state.handle, record)
            state.records += 1
            if state.records >= self.chunk_records:
                self._finalize(state)
                self._chunks.pop(key, None)
                return state.final_path
            return state.open_path

    def _new_chunk_state(self, partition: Path, now: datetime) -> "ChunkState":
        stem = f"{now.strftime('%H%M%S%f')}-{os.getpid()}"
        final_path = partition / f"{stem}.jsonl.gz"
        return ChunkState(
            open_path=partition / f"{stem}.jsonl.gz.open",
            final_path=final_path,
            handle=gzip.open(
                partition / f"{stem}.jsonl.gz.open",
                "wt",
                encoding="utf-8",
                compresslevel=1,
            ),
            records=0,
            opened_at=time.monotonic(),
        )

    def _partition(self, source: str, entity: str, now: datetime) -> Path:
        partition = self.root / f"source={source}" / f"entity={entity}" / f"dt={now.date()}"
        partition.mkdir(parents=True, exist_ok=True)
        return partition

    def flush(self) -> list[Path]:
        finalized: list[Path] = []
        with self._lock:
            for key, state in list(self._chunks.items()):
                if state.records > 0:
                    finalized.append(self._finalize(state))
                else:
                    state.handle.close()
                    state.open_path.unlink(missing_ok=True)
                self._chunks.pop(key, None)
        return finalized

    def _finalize(self, state: "ChunkState") -> Path:
        state.handle.close()
        state.open_path.replace(state.final_path)
        return state.final_path

    def _should_finalize(self, state: "ChunkState") -> bool:
        return state.records >= self.chunk_records or (
            state.records > 0 and time.monotonic() - state.opened_at >= self.chunk_seconds
        )


@dataclass
class ChunkState:
    open_path: Path
    final_path: Path
    handle: TextIO
    records: int
    opened_at: float


def write_record(handle, record: RawRecord) -> None:
    handle.write(json.dumps(asdict(record), ensure_ascii=False, separators=(",", ":")))
    handle.write("\n")
