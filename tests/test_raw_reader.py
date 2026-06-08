import time

from zetta.storage.raw import RawJsonlWriter
from zetta.storage.raw_reader import iter_raw_paths, iter_raw_records


def test_iter_raw_records_filters_by_source_entity_and_after_path(tmp_path) -> None:
    writer = RawJsonlWriter(tmp_path)
    first = writer.write(
        source="data",
        entity="trades",
        request_url="https://data.test/trades?page=1",
        payload=[{"id": 1}],
    )
    time.sleep(0.001)
    second = writer.write(
        source="data",
        entity="trades",
        request_url="https://data.test/trades?page=2",
        payload=[{"id": 2}],
    )
    writer.write(
        source="gamma",
        entity="markets",
        request_url="https://gamma.test/markets",
        payload={"markets": []},
    )

    paths = list(iter_raw_paths(tmp_path, source="data", entity="trades", after_path=str(first)))
    records = list(iter_raw_records(tmp_path, source="data", entity="trades", after_path=str(first)))

    assert paths == [second]
    assert [record["request_url"] for record in records] == ["https://data.test/trades?page=2"]


def test_iter_raw_records_reads_all_records_in_chunk(tmp_path) -> None:
    writer = RawJsonlWriter(tmp_path, chunk_records=2)
    writer.write(source="clob_ws", entity="market", request_url="u1", payload={"n": 1})
    final_path = writer.write(source="clob_ws", entity="market", request_url="u2", payload={"n": 2})

    records = list(iter_raw_records(tmp_path, source="clob_ws", entity="market"))

    assert final_path.suffix == ".gz"
    assert [record["payload"]["n"] for record in records] == [1, 2]
    assert [record["_raw_path"] for record in records] == [
        f"{final_path}#000000001",
        f"{final_path}#000000002",
    ]
