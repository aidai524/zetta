from datetime import datetime

from zetta.loaders.chain import ChainRawLoader, chain_log_row, hex_int
from zetta.storage.raw import RawJsonlWriter


class FakeClickHouse:
    def __init__(self) -> None:
        self.tables = {}
        self.loaded_hashes = set()

    def insert(self, table, rows):
        self.tables.setdefault(table, []).extend(rows)
        if table == "raw_ingest_log":
            self.loaded_hashes.update(row["payload_hash"] for row in rows)
        return len(rows)

    def query_text(self, query):
        if "raw_ingest_log" in query:
            return "\n".join(sorted(self.loaded_hashes))
        return ""


def test_hex_int_accepts_hex_decimal_and_missing_values() -> None:
    assert hex_int("0x10") == 16
    assert hex_int("17") == 17
    assert hex_int(None) == 0


def test_chain_log_row_normalizes_rpc_log() -> None:
    row = chain_log_row(
        {
            "blockNumber": "0x10",
            "blockHash": "0xblock",
            "transactionHash": "0xtx",
            "logIndex": "0x2",
            "address": "0xABC",
            "topics": ["0xtopic0", "0xtopic1"],
            "data": "0xdata",
            "removed": False,
        },
        ingested_at=datetime(2026, 1, 1),
    )

    assert row["chain_id"] == 137
    assert row["block_number"] == 16
    assert row["log_index"] == 2
    assert row["address"] == "0xabc"
    assert row["topic0"] == "0xtopic0"


def test_chain_raw_loader_writes_logs_and_deduplicates(tmp_path) -> None:
    payload = {
        "from_block": 10,
        "to_block": 10,
        "logs": [
            {
                "blockNumber": "0xa",
                "transactionHash": "0xtx",
                "logIndex": "0x0",
                "address": "0xabc",
                "topics": ["0xtopic0"],
                "data": "0x",
            }
        ],
    }
    RawJsonlWriter(tmp_path).write(
        source="polygon",
        entity="logs",
        request_url="https://polygon-rpc.test",
        payload=payload,
    )
    RawJsonlWriter(tmp_path).write(
        source="polygon",
        entity="logs",
        request_url="https://polygon-rpc.test",
        payload=payload,
    )
    fake = FakeClickHouse()

    result = ChainRawLoader(clickhouse=fake).load_logs(raw_root=tmp_path)

    assert result.raw_records == 2
    assert result.skipped_raw_records == 1
    assert result.chain_logs == 1
    assert result.ingest_logs == 1
    assert fake.tables["fact_chain_log"][0]["block_number"] == 10
