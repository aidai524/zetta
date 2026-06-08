import time

from zetta.loaders.data import (
    DataRawLoader,
    activity_rows,
    holder_rows,
    market_position_rows,
    open_interest_rows,
    trade_id,
)
from zetta.storage.raw import RawJsonlWriter


class FakeClickHouse:
    def __init__(self) -> None:
        self.tables = {}
        self.loaded_hashes = set()
        self.max_raw_path = ""
        self.queries = []

    def insert(self, table, rows):
        self.tables.setdefault(table, []).extend(rows)
        if table == "raw_ingest_log":
            self.loaded_hashes.update(row["payload_hash"] for row in rows)
        return len(rows)

    def query_text(self, query):
        self.queries.append(query)
        if "max(raw_path)" in query:
            return self.max_raw_path
        if "startsWith(raw_path" in query:
            return ""
        if "raw_ingest_log" in query:
            return "\n".join(sorted(self.loaded_hashes))
        return ""


def test_trade_id_is_stable() -> None:
    first = trade_id("tx", "token", __import__("datetime").datetime(2026, 1, 1), 0)
    second = trade_id("tx", "token", __import__("datetime").datetime(2026, 1, 1), 0)

    assert first == second


def test_data_trades_loader_writes_fact_rows(tmp_path) -> None:
    RawJsonlWriter(tmp_path).write(
        source="data",
        entity="trades",
        request_url="https://data.test/trades?limit=1",
        payload=[
            {
                "proxyWallet": "0xABC",
                "side": "BUY",
                "asset": "token-1",
                "conditionId": "condition-1",
                "size": 2,
                "price": 0.25,
                "timestamp": 1780565328,
                "transactionHash": "0xTX",
            }
        ],
    )
    fake = FakeClickHouse()

    result = DataRawLoader(clickhouse=fake).load_trades(raw_root=tmp_path, batch_size=1)

    assert result.raw_records == 1
    assert result.trades == 1
    row = fake.tables["fact_trade"][0]
    assert row["token_id"] == "token-1"
    assert row["user_address"] == "0xabc"
    assert row["notional"] == 0.5


def test_activity_rows_normalize_user_activity() -> None:
    rows = activity_rows(
        [
            {
                "proxyWallet": "0xABC",
                "timestamp": 1780566319,
                "type": "TRADE",
                "conditionId": "condition-1",
                "asset": "token-1",
                "transactionHash": "0xTX",
                "side": "BUY",
                "price": 0.4,
                "size": 3.3,
                "usdcSize": 1.32,
            }
        ],
        __import__("datetime").datetime(2026, 1, 1),
    )

    assert rows[0]["activity_type"] == "TRADE"
    assert rows[0]["notional"] == 1.32


def test_holder_rows_flatten_token_groups() -> None:
    rows = holder_rows(
        [
            {
                "token": "token-1",
                "holders": [
                    {
                        "proxyWallet": "0xABC",
                        "amount": 10,
                        "outcomeIndex": 1,
                        "pseudonym": "Name",
                        "verified": True,
                    }
                ],
            }
        ],
        __import__("datetime").datetime(2026, 1, 1),
    )

    assert rows[0]["token_id"] == "token-1"
    assert rows[0]["amount"] == 10.0
    assert rows[0]["verified"] is True


def test_market_position_rows_flatten_positions() -> None:
    rows = market_position_rows(
        [
            {
                "token": "token-1",
                "positions": [
                    {
                        "proxyWallet": "0xABC",
                        "conditionId": "condition-1",
                        "size": 2,
                        "avgPrice": 0.25,
                        "currPrice": 0.5,
                        "currentValue": 1,
                        "cashPnl": 0.5,
                        "realizedPnl": 0.1,
                        "totalPnl": 0.6,
                        "totalBought": 2,
                        "outcome": "Yes",
                        "outcomeIndex": 0,
                    }
                ],
            }
        ],
        __import__("datetime").datetime(2026, 1, 1),
    )

    assert rows[0]["condition_id"] == "condition-1"
    assert rows[0]["total_pnl"] == 0.6


def test_open_interest_rows_normalize_values() -> None:
    rows = open_interest_rows(
        [{"market": "condition-1", "value": 123.45}],
        __import__("datetime").datetime(2026, 1, 1),
    )

    assert rows[0]["condition_id"] == "condition-1"
    assert rows[0]["value"] == 123.45


def test_data_trades_loader_uses_raw_path_high_watermark(tmp_path) -> None:
    writer = RawJsonlWriter(tmp_path)
    first = writer.write(
        source="data",
        entity="trades",
        request_url="https://data.test/trades?page=1",
        payload=[],
    )
    time.sleep(0.001)
    writer.write(
        source="data",
        entity="trades",
        request_url="https://data.test/trades?page=2",
        payload=[],
    )
    fake = FakeClickHouse()
    fake.max_raw_path = str(first)

    result = DataRawLoader(clickhouse=fake).load_trades(raw_root=tmp_path)

    assert result.raw_records == 1
    assert any("max(raw_path)" in query for query in fake.queries)
    assert not any("SELECT payload_hash" in query for query in fake.queries)
