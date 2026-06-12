import time

from zetta.loaders.data import (
    DataRawLoader,
    activity_rows,
    holder_rows,
    market_position_rows,
    open_interest_rows,
    trade_id,
    wallet_portfolio_rows,
)
from zetta.storage.raw import RawJsonlWriter


class FakeClickHouse:
    def __init__(self) -> None:
        self.tables = {}
        self.loaded_hashes = set()
        self.max_raw_path = ""
        self.queries = []
        self.settings = None

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
    indexed_row = fake.tables["fact_trade_by_user"][0]
    time_indexed_row = fake.tables["fact_trade_by_time"][0]
    assert row["token_id"] == "token-1"
    assert row["user_address"] == "0xabc"
    assert row["notional"] == 0.5
    assert indexed_row == row
    assert time_indexed_row == row


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


def test_market_position_rows_accept_wallet_positions() -> None:
    rows = market_position_rows(
        [
            {
                "proxyWallet": "0xABC",
                "asset": "token-1",
                "conditionId": "condition-1",
                "size": 2,
                "avgPrice": 0.25,
                "curPrice": 0.5,
                "currentValue": 1,
                "cashPnl": 0.5,
                "realizedPnl": 0.1,
                "totalBought": 2,
                "outcome": "Yes",
                "outcomeIndex": 0,
            }
        ],
        __import__("datetime").datetime(2026, 1, 1),
    )

    assert rows[0]["token_id"] == "token-1"
    assert rows[0]["user_address"] == "0xabc"
    assert rows[0]["total_pnl"] == 0.6


def test_wallet_portfolio_rows_from_aggregate() -> None:
    rows = wallet_portfolio_rows(
        {
            "user": "0xABC",
            "positions": [
                {"proxyWallet": "0xABC", "currentValue": 24.2423},
                {"proxyWallet": "0xABC", "currentValue": 0, "redeemable": True},
            ],
            "value": [{"user": "0xABC", "value": 24.2423}],
            "pnl": [{"t": 1781254800, "p": 154.27983}],
            "availableBalance": 1.202269,
        },
        __import__("datetime").datetime(2026, 1, 1),
    )

    assert rows[0]["position_count"] == 1
    assert rows[0]["positions_value"] == 24.2423
    assert rows[0]["available_balance"] == 1.202269
    assert rows[0]["portfolio_value"] == 25.444569
    assert rows[0]["total_pnl"] == 154.27983


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


def test_data_trades_loader_can_load_latest_files_first_with_limit(tmp_path) -> None:
    writer = RawJsonlWriter(tmp_path)
    first = writer.write(
        source="data",
        entity="trades",
        request_url="https://data.test/trades?page=1",
        payload=[
            {
                "asset": "token-old",
                "conditionId": "condition-1",
                "price": 0.25,
                "proxyWallet": "0xABC",
                "side": "BUY",
                "size": 1,
                "timestamp": 1780565328,
                "transactionHash": "0xOLD",
            }
        ],
    )
    time.sleep(0.001)
    second = writer.write(
        source="data",
        entity="trades",
        request_url="https://data.test/trades?page=2",
        payload=[
            {
                "asset": "token-new",
                "conditionId": "condition-2",
                "price": 0.5,
                "proxyWallet": "0xDEF",
                "side": "SELL",
                "size": 2,
                "timestamp": 1780566328,
                "transactionHash": "0xNEW",
            }
        ],
    )
    fake = FakeClickHouse()
    fake.max_raw_path = str(first)

    result = DataRawLoader(clickhouse=fake).load_trades(
        raw_root=tmp_path,
        batch_size=1,
        max_paths=1,
        newest_first=True,
    )

    assert result.raw_records == 1
    assert fake.tables["fact_trade"][0]["token_id"] == "token-new"
    assert fake.tables["fact_trade_by_user"][0]["token_id"] == "token-new"
    assert fake.tables["fact_trade_by_time"][0]["token_id"] == "token-new"
    assert fake.tables["raw_ingest_log"][0]["raw_path"] == f"{second}#000000001"
    assert not any("max(raw_path)" in query for query in fake.queries)
