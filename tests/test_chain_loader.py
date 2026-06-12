from datetime import datetime

from zetta.chain.polymarket import TRANSFER_SINGLE_TOPIC
from zetta.loaders.chain import ChainDecodeCheckpoint, ChainRawLoader, chain_log_row, hex_int
from zetta.storage.raw import RawJsonlWriter


class FakeClickHouse:
    def __init__(self) -> None:
        self.tables = {}
        self.loaded_hashes = set()
        self.queries = []
        self.settings = None
        self.checkpoint = None
        self.source_checkpoint = None
        self.chain_logs = []

    def insert(self, table, rows):
        self.tables.setdefault(table, []).extend(rows)
        if table == "raw_ingest_log":
            self.loaded_hashes.update(row["payload_hash"] for row in rows)
        return len(rows)

    def query_text(self, query):
        self.queries.append(query)
        if "from fact_ctf_balance_movement" in query and self.checkpoint is not None:
            return checkpoint_row(self.checkpoint)
        if "from fact_chain_log" in query and "topic0 =" in query:
            return "\n".join(self.chain_logs)
        if "from fact_chain_log" in query and self.source_checkpoint is not None:
            return checkpoint_row(self.source_checkpoint)
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


def test_chain_decoded_loader_uses_existing_table_checkpoint() -> None:
    fake = FakeClickHouse()
    fake.checkpoint = ChainDecodeCheckpoint(
        block_number=10,
        transaction_hash="0xaaa",
        log_index=1,
    )

    result = ChainRawLoader(clickhouse=fake).build_balance_movements()

    assert result.balance_movements == 0
    query = next(query for query in fake.queries if "topic0 =" in query)
    assert "(block_number, transaction_hash, log_index) >" in query
    assert "(10, '0xaaa', 1)" in query


def test_chain_decoded_loader_saves_source_checkpoint_when_no_new_rows(tmp_path) -> None:
    fake = FakeClickHouse()
    fake.settings = type("Settings", (), {"state_dir": tmp_path})()
    fake.source_checkpoint = ChainDecodeCheckpoint(
        block_number=20,
        transaction_hash="0xbbb",
        log_index=3,
    )

    ChainRawLoader(clickhouse=fake).build_decoded_events(
        topic=TRANSFER_SINGLE_TOPIC,
        table="fact_ctf_balance_movement",
        row_builder=lambda _log, **_kwargs: [],
        batch_size=10,
    )

    saved = ChainRawLoader(clickhouse=fake).saved_decode_checkpoint(
        "fact_ctf_balance_movement",
        TRANSFER_SINGLE_TOPIC,
    )
    assert saved == fake.source_checkpoint


def checkpoint_row(checkpoint: ChainDecodeCheckpoint) -> str:
    return (
        '{"block_number":'
        f"{checkpoint.block_number},"
        '"transaction_hash":'
        f'"{checkpoint.transaction_hash}",'
        '"log_index":'
        f"{checkpoint.log_index}"
        "}"
    )
