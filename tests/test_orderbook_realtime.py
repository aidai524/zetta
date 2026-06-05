from zetta.realtime.orderbook import (
    OrderBookReconstructor,
    numeric_delta,
    parse_market_timestamp,
    reconciliation_diff,
    reconstruct_ws_market_raw,
    rest_book_summary,
)
from zetta.storage.raw import RawJsonlWriter


def test_orderbook_reconstructor_applies_book_and_price_changes() -> None:
    reconstructor = OrderBookReconstructor()
    reconstructor.apply_event(
        {
            "event_type": "book",
            "asset_id": "token-1",
            "market": "condition-1",
            "timestamp": "1780565328397",
            "bids": [{"price": "0.4", "size": "10"}],
            "asks": [{"price": "0.6", "size": "8"}],
        }
    )
    reconstructor.apply_event(
        {
            "event_type": "price_change",
            "timestamp": "1780565329397",
            "price_changes": [
                {"asset_id": "token-1", "side": "BUY", "price": "0.5", "size": "4"},
                {"asset_id": "token-1", "side": "SELL", "price": "0.6", "size": "0"},
                {"asset_id": "token-1", "side": "SELL", "price": "0.7", "size": "3"},
            ],
        }
    )

    summary = reconstructor.books["token-1"].summary()
    assert summary["best_bid"] == 0.5
    assert summary["best_ask"] == 0.7
    assert summary["bid_depth"] == 14.0
    assert summary["ask_depth"] == 3.0


def test_reconstruct_ws_market_raw_replays_events(tmp_path) -> None:
    RawJsonlWriter(tmp_path).write(
        source="clob_ws",
        entity="market",
        request_url="wss://example.test/ws",
        payload=[
            {
                "event_type": "book",
                "asset_id": "token-1",
                "bids": [{"price": "0.1", "size": "1"}],
                "asks": [{"price": "0.9", "size": "2"}],
            }
        ],
    )

    reconstructor = reconstruct_ws_market_raw(raw_root=tmp_path)

    assert reconstructor.books["token-1"].best_bid == 0.1
    assert reconstructor.books["token-1"].best_ask == 0.9


def test_rest_book_summary_and_reconciliation_diff() -> None:
    rest = rest_book_summary(
        {
            "asset_id": "token-1",
            "bids": [{"price": "0.3", "size": "5"}],
            "asks": [{"price": "0.8", "size": "7"}],
        }
    )
    diff = reconciliation_diff(
        reconstructed={"token_id": "token-1", "best_bid": 0.4, "best_ask": 0.8},
        rest=rest,
    )

    assert rest["bid_depth"] == 5.0
    assert round(numeric_delta(0.4, 0.3), 6) == 0.1
    assert diff["best_bid_delta"] == 0.10000000000000003
    assert diff["best_ask_delta"] == 0.0


def test_parse_market_timestamp_accepts_millisecond_strings() -> None:
    assert parse_market_timestamp("1780565328397").year == 2026
