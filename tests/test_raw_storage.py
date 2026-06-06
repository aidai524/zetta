import gzip
import json

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
