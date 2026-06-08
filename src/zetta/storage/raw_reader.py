from __future__ import annotations

import gzip
import json
import zlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any


def iter_raw_records(
    root: Path,
    *,
    source: str | None = None,
    entity: str | None = None,
    after_path: str | None = None,
) -> Iterator[dict[str, Any]]:
    for path in iter_raw_paths(root, source=source, entity=entity, after_path=after_path):
        yield from iter_raw_records_from_paths([path], after_path=after_path)


def iter_raw_records_from_paths(
    paths: list[Path] | list[str],
    *,
    after_path: str | None = None,
) -> Iterator[dict[str, Any]]:
    for path_value in paths:
        path = Path(path_value)
        try:
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                for line_number, line in enumerate(handle, start=1):
                    if line.strip():
                        record_path = raw_record_path(path, line_number)
                        if after_path and record_path <= after_path:
                            continue
                        record = json.loads(line)
                        record["_raw_path"] = record_path
                        yield record
        except (EOFError, OSError, zlib.error) as exc:
            raise RuntimeError(f"Failed to read raw gzip file {path}: {exc}") from exc


def iter_raw_paths(
    root: Path,
    *,
    source: str | None = None,
    entity: str | None = None,
    after_path: str | None = None,
) -> Iterator[Path]:
    if source and entity:
        base = root / f"source={source}" / f"entity={entity}"
        yield from iter_entity_paths(base, after_path=after_path)
        return

    base = root / f"source={source}" if source else root
    if not base.exists():
        return
    after_file = raw_file_path(after_path) if after_path and is_raw_record_path(after_path) else None
    for path in sorted(base.rglob("*.jsonl.gz")):
        path_text = str(path)
        if entity and f"entity={entity}" not in path_text:
            continue
        if after_path and after_file and path_text < after_file:
            continue
        if after_path and after_file is None and path_text <= after_path:
            continue
        yield path


def iter_entity_paths(base: Path, *, after_path: str | None = None) -> Iterator[Path]:
    if not base.exists():
        return
    after_file = raw_file_path(after_path) if after_path and is_raw_record_path(after_path) else None
    after_dir = str(Path(after_file or after_path).parent) if after_path else None
    for partition in sorted(base.glob("dt=*")):
        if not partition.is_dir():
            continue
        if after_dir and str(partition) < after_dir:
            continue
        for path in sorted(partition.glob("*.jsonl.gz")):
            path_text = str(path)
            if after_path and after_file and path_text < after_file:
                continue
            if after_path and after_file is None and path_text <= after_path:
                continue
            yield path


def raw_record_path(path: Path, line_number: int) -> str:
    return f"{path}#{line_number:09d}"


def raw_file_path(path: str | None) -> str | None:
    if path is None:
        return None
    return path.split("#", 1)[0]


def is_raw_record_path(path: str) -> bool:
    return ".jsonl.gz#" in path
