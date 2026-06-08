from zetta.loaders.clob import ClobRawLoader, query_param, side_depth, ws_market_events
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
        if "startsWith(raw_path" in query:
            return ""
        if "raw_ingest_log" in query:
            return "\n".join(sorted(self.loaded_hashes))
        return ""


def test_query_param_extracts_market_token() -> None:
    assert query_param("https://clob.test/prices-history?market=abc&interval=all", "market") == "abc"


def test_clob_price_history_loader_writes_fact_rows(tmp_path) -> None:
    RawJsonlWriter(tmp_path).write(
        source="clob",
        entity="prices_history",
        request_url="https://clob.test/prices-history?market=token-1&interval=all",
        payload={"history": [{"t": 1777886405, "p": 0.44}, {"t": 1777887005, "p": 0.45}]},
    )
    fake = FakeClickHouse()

    result = ClobRawLoader(clickhouse=fake).load_price_history(raw_root=tmp_path, batch_size=1)

    assert result.raw_records == 1
    assert result.price_history == 2
    assert result.ingest_logs == 1
    assert fake.tables["fact_price_history"][0]["token_id"] == "token-1"
    assert fake.tables["fact_price_history"][1]["price"] == 0.45


def test_clob_price_history_loader_skips_duplicate_payload(tmp_path) -> None:
    payload = {"history": [{"t": 1777886405, "p": 0.44}]}
    RawJsonlWriter(tmp_path).write(
        source="clob",
        entity="prices_history",
        request_url="https://clob.test/prices-history?market=token-1&interval=all",
        payload=payload,
    )
    RawJsonlWriter(tmp_path).write(
        source="clob",
        entity="prices_history",
        request_url="https://clob.test/prices-history?market=token-1&interval=all",
        payload=payload,
    )
    fake = FakeClickHouse()

    result = ClobRawLoader(clickhouse=fake).load_price_history(raw_root=tmp_path)

    assert result.raw_records == 2
    assert result.skipped_raw_records == 1
    assert result.price_history == 1


def test_side_depth_sums_level_sizes() -> None:
    assert side_depth([{"size": "1.5"}, {"size": "2"}]) == 3.5


def test_clob_book_loader_writes_snapshot(tmp_path) -> None:
    RawJsonlWriter(tmp_path).write(
        source="clob",
        entity="book",
        request_url="https://clob.test/book?token_id=token-1",
        payload={
            "market": "condition-1",
            "asset_id": "token-1",
            "timestamp": "1780565328397",
            "bids": [{"price": "0.1", "size": "10"}, {"price": "0.2", "size": "5"}],
            "asks": [{"price": "0.4", "size": "8"}, {"price": "0.3", "size": "7"}],
        },
    )
    fake = FakeClickHouse()

    result = ClobRawLoader(clickhouse=fake).load_books(raw_root=tmp_path)

    assert result.raw_records == 1
    assert result.orderbook_snapshots == 1
    row = fake.tables["fact_orderbook_snapshot"][0]
    assert row["token_id"] == "token-1"
    assert row["best_bid"] == 0.2
    assert row["best_ask"] == 0.3
    assert row["bid_depth"] == 15.0
    assert row["ask_depth"] == 15.0


def test_ws_market_events_accepts_batch_and_single_payloads() -> None:
    event = {"event_type": "book", "asset_id": "token-1"}

    assert ws_market_events([event, "skip"]) == [event]
    assert ws_market_events(event) == [event]
    assert ws_market_events("PONG") == []


def test_clob_ws_market_book_loader_writes_snapshots_from_batched_payload(tmp_path) -> None:
    RawJsonlWriter(tmp_path).write(
        source="clob_ws",
        entity="market",
        request_url="wss://ws-subscriptions-clob.polymarket.com/ws/market",
        payload=[
            {
                "event_type": "book",
                "market": "condition-1",
                "asset_id": "token-1",
                "timestamp": "1780565328397",
                "bids": [{"price": "0.1", "size": "10"}],
                "asks": [{"price": "0.4", "size": "8"}],
            },
            {
                "event_type": "price_change",
                "asset_id": "token-1",
                "changes": [{"side": "BUY", "price": "0.2", "size": "3"}],
            },
            {
                "event_type": "book",
                "market": "condition-2",
                "asset_id": "token-2",
                "timestamp": "1780565329397",
                "bids": [{"price": "0.2", "size": "5"}],
                "asks": [{"price": "0.7", "size": "2"}],
            },
        ],
    )
    fake = FakeClickHouse()

    result = ClobRawLoader(clickhouse=fake).load_ws_market_books(raw_root=tmp_path)

    assert result.raw_records == 1
    assert result.orderbook_snapshots == 2
    assert result.ingest_logs == 1
    assert fake.tables["raw_ingest_log"][0]["source"] == "clob_ws"
    assert fake.tables["raw_ingest_log"][0]["item_count"] == 3
    rows = fake.tables["fact_orderbook_snapshot"]
    assert [row["token_id"] for row in rows] == ["token-1", "token-2"]
    assert rows[0]["best_bid"] == 0.1
    assert rows[1]["best_ask"] == 0.7
