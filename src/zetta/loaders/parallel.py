from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from dataclasses import fields, is_dataclass
from pathlib import Path
from typing import Any, TypeVar

from zetta.storage.raw_reader import iter_raw_paths


T = TypeVar("T")


def raw_paths(
    raw_root: Path,
    *,
    source: str,
    entity: str,
    after_path: str | None,
    max_paths: int | None = None,
    newest_first: bool = False,
) -> list[str]:
    paths: list[str] = []
    for path in iter_raw_paths(
        raw_root,
        source=source,
        entity=entity,
        after_path=after_path,
        newest_first=newest_first,
    ):
        paths.append(str(path))
        if max_paths is not None and len(paths) >= max_paths:
            break
    return paths


def load_in_parallel(
    *,
    worker: Callable[[list[str]], T],
    paths: list[str],
    workers: int,
) -> T | None:
    if workers <= 1 or len(paths) <= 1:
        return worker(paths)
    chunks = chunk_paths(paths, workers)
    with ProcessPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(worker, chunks))
    return merge_results(results)


def chunk_paths(paths: list[str], workers: int) -> list[list[str]]:
    workers = max(1, min(workers, len(paths)))
    return [paths[index::workers] for index in range(workers)]


def merge_results(results: list[T]) -> T | None:
    if not results:
        return None
    first = results[0]
    if not is_dataclass(first):
        raise TypeError("parallel loader result must be a dataclass")
    values: dict[str, Any] = {}
    for field in fields(first):
        first_value = getattr(first, field.name)
        if isinstance(first_value, str):
            values[field.name] = max(str(getattr(result, field.name)) for result in results)
            continue
        total = 0
        for result in results:
            total += int(getattr(result, field.name))
        values[field.name] = total
    return type(first)(**values)
