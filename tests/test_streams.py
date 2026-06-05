from zetta.storage.raw import RawJsonlWriter
from zetta.storage.redpanda import PublishResult
from zetta.streams import publish_ws_market_raw, ws_market_stream_messages


class FakePublisher:
    def __init__(self) -> None:
        self.created_topics = []
        self.published = []

    def create_topic(self, topic):
        self.created_topics.append(topic)
        return True

    def publish_json(self, *, topic, messages):
        self.published.extend(messages)
        return PublishResult(topic=topic, messages=len(messages))


def test_ws_market_stream_messages_wraps_each_event() -> None:
    messages = ws_market_stream_messages(
        {
            "collected_at": "2026-06-04T00:00:00+00:00",
            "request_url": "wss://example.test/ws",
            "payload": [{"event_type": "book", "asset_id": "token-1"}],
        }
    )

    assert messages == [
        {
            "source": "clob_ws",
            "channel": "market",
            "collected_at": "2026-06-04T00:00:00+00:00",
            "request_url": "wss://example.test/ws",
            "event": {"event_type": "book", "asset_id": "token-1"},
        }
    ]


def test_publish_ws_market_raw_publishes_batched_events(tmp_path) -> None:
    RawJsonlWriter(tmp_path).write(
        source="clob_ws",
        entity="market",
        request_url="wss://example.test/ws",
        payload=[
            {"event_type": "book", "asset_id": "token-1"},
            {"event_type": "book", "asset_id": "token-2"},
        ],
    )
    publisher = FakePublisher()

    result = publish_ws_market_raw(
        raw_root=tmp_path,
        publisher=publisher,
        topic="topic-1",
        batch_size=1,
    )

    assert publisher.created_topics == ["topic-1"]
    assert result.raw_records == 1
    assert result.events == 2
    assert result.published == 2
    assert [message["event"]["asset_id"] for message in publisher.published] == [
        "token-1",
        "token-2",
    ]
