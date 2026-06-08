from zetta.loaders.incremental import (
    loaded_payload_hashes_for_paths,
    loader_checkpoint_raw_path,
    save_loader_checkpoint_raw_path,
)


class FakeClickHouse:
    def __init__(self) -> None:
        self.queries = []

    def query_text(self, query):
        self.queries.append(query)
        return "hash-1\n"


def test_loader_checkpoint_round_trips_raw_path(tmp_path) -> None:
    raw_path = "/data/raw/source=data/entity=trades/dt=2026-06-08/120000000000.jsonl.gz#000000002"

    save_loader_checkpoint_raw_path(
        tmp_path,
        source="data",
        entity="trades",
        raw_path=raw_path,
    )

    assert loader_checkpoint_raw_path(tmp_path, source="data", entity="trades") == raw_path


def test_loaded_payload_hashes_for_paths_scopes_query_to_raw_paths() -> None:
    clickhouse = FakeClickHouse()

    hashes = loaded_payload_hashes_for_paths(
        clickhouse,
        source="data",
        entity="trades",
        paths=["/tmp/a.jsonl.gz", "/tmp/b's.jsonl.gz"],
    )

    assert hashes == {"hash-1"}
    assert "startsWith(raw_path" in clickhouse.queries[0]
    assert "/tmp/a.jsonl.gz" in clickhouse.queries[0]
    assert "/tmp/b\\'s.jsonl.gz" in clickhouse.queries[0]
