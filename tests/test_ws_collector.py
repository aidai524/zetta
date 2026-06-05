from zetta.collectors.ws import is_heartbeat, parse_ws_payload, subscription_message


def test_subscription_message_matches_polymarket_market_channel() -> None:
    message = subscription_message(["token-1", "token-2"])

    assert message == {
        "assets_ids": ["token-1", "token-2"],
        "type": "market",
        "custom_feature_enabled": True,
    }


def test_parse_ws_payload_handles_json_bytes_and_plain_messages() -> None:
    assert parse_ws_payload(b'{"event_type":"price_change"}') == {
        "event_type": "price_change"
    }
    assert parse_ws_payload("PONG") == {"message": "PONG"}


def test_is_heartbeat_detects_pong_messages() -> None:
    assert is_heartbeat({"message": "PONG"})
    assert is_heartbeat({"message": "pong"})
    assert not is_heartbeat({"event_type": "book"})
