from dataclasses import dataclass

from zetta.loaders.parallel import chunk_paths, load_in_parallel


@dataclass(frozen=True)
class CountResult:
    raw_records: int
    skipped_raw_records: int


class CountWorker:
    def __call__(self, paths: list[str]) -> CountResult:
        return CountResult(raw_records=len(paths), skipped_raw_records=0)


def test_chunk_paths_stripes_work_across_workers() -> None:
    assert chunk_paths(["a", "b", "c", "d", "e"], workers=2) == [["a", "c", "e"], ["b", "d"]]


def test_load_in_parallel_merges_dataclass_counts() -> None:
    result = load_in_parallel(worker=CountWorker(), paths=["a", "b", "c"], workers=2)

    assert result == CountResult(raw_records=3, skipped_raw_records=0)
