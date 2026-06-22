from zetta.realtime_trades import trade_stream_message, trade_stream_row_key


def test_trade_stream_row_key_prefers_trade_id() -> None:
    assert trade_stream_row_key({"trade_id": "abc", "transaction_hash": "0x1"}) == "id:abc"


def test_trade_stream_row_key_uses_transaction_hash_tuple() -> None:
    row = {
        "transaction_hash": "0xabc",
        "token_id": "token-1",
        "user_address": "0xwallet",
        "side": "BUY",
        "price": 0.42,
        "size": 10,
    }

    assert trade_stream_row_key(row) == "tx|0xabc|token-1|0xwallet|BUY|0.42|10"


def test_trade_stream_message_contains_frontend_trade_payload() -> None:
    row = {
        "trade_id": "trade-1",
        "timestamp": "2026-06-22T01:02:03+00:00",
        "transaction_hash": "0xabc",
        "token_id": "token-1",
        "condition_id": "condition-1",
        "user_address": "0xwallet",
        "side": "buy",
        "price": "0.50",
        "size": "20",
        "question": "Will it happen?",
        "market_slug": "market-slug",
        "outcome": "Yes",
    }

    message = trade_stream_message(row, source="chain", captured_at="2026-06-22T01:02:04+00:00")

    assert message["type"] == "trade"
    assert message["key"] == "id:trade-1"
    assert message["side"] == "BUY"
    assert message["price"] == 0.5
    assert message["size"] == 20.0
    assert message["notional"] == 10.0
    assert message["asset_id"] == "token-1"
    assert message["maker_address"] == "0xwallet"
    assert message["trade"] is row
