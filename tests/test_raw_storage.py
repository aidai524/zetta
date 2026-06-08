import gzip
import json
import time

from zetta.storage.raw import RawJsonlWriter


def test_raw_jsonl_writer_writes_partitioned_gzip(tmp_path) -> None:
    output_path = RawJsonlWriter(tmp_path).write(
        source="gamma",
        entity="events",
        request_url="https://example.test/events",
        payload={"data": [{"id": "1"}]},
    )

    assert output_path.exists()
    assert "source=gamma" in str(output_path)
    assert "entity=events" in str(output_path)

    with gzip.open(output_path, "rt", encoding="utf-8") as handle:
        record = json.loads(handle.readline())

    assert record["source"] == "gamma"
    assert record["entity"] == "events"
    assert record["payload"] == {"data": [{"id": "1"}]}
    assert not list(tmp_path.rglob("*.tmp"))


def test_raw_jsonl_writer_flushes_chunked_gzip(tmp_path) -> None:
    writer = RawJsonlWriter(tmp_path, chunk_records=3)
    first_path = writer.write(
        source="clob_ws",
        entity="market",
        request_url="wss://example.test",
        payload={"event_type": "price_change"},
    )
    writer.write(
        source="clob_ws",
        entity="market",
        request_url="wss://example.test",
        payload={"event_type": "book"},
    )

    assert first_path.suffix == ".open"
    assert not list(tmp_path.rglob("*.jsonl.gz"))

    finalized = writer.flush()

    assert len(finalized) == 1
    assert finalized[0].suffix == ".gz"
    assert not list(tmp_path.rglob("*.open"))
    with gzip.open(finalized[0], "rt", encoding="utf-8") as handle:
        records = [json.loads(line) for line in handle if line.strip()]
    assert [record["payload"]["event_type"] for record in records] == ["price_change", "book"]


def test_raw_jsonl_writer_rotates_stale_chunk(tmp_path) -> None:
    writer = RawJsonlWriter(tmp_path, chunk_records=10, chunk_seconds=0.001)
    first = writer.write(
        source="data",
        entity="trades",
        request_url="u1",
        payload={"item": 1},
    )
    time.sleep(0.002)
    second = writer.write(
        source="data",
        entity="trades",
        request_url="u2",
        payload={"item": 2},
    )

    assert first.suffix == ".open"
    assert second.suffix == ".open"
    assert len(list(tmp_path.rglob("*.jsonl.gz"))) == 1
