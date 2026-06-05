from zetta.collectors.gamma import GammaCollector
from zetta.http import HttpResponse
from zetta.polymarket import Page
from zetta.storage.state import LocalStateStore


class FakeRawWriter:
    def __init__(self) -> None:
        self.records = []

    def write(self, **record):
        self.records.append(record)
        return "raw.jsonl.gz"


class StuckCursorClient:
    def gamma_events_keyset(self, **_kwargs):
        return Page(
            response=HttpResponse(
                url="https://example.test/events/keyset",
                status=200,
                headers={},
                body={"events": [{"id": "1"}], "next_cursor": "same"},
            ),
            items=[{"id": "1"}],
            next_cursor="same",
        )


def test_gamma_collector_stops_when_cursor_does_not_advance(tmp_path) -> None:
    raw_writer = FakeRawWriter()
    collector = GammaCollector(
        client=StuckCursorClient(),
        raw_writer=raw_writer,
        state_store=LocalStateStore(tmp_path),
    )

    result = collector.collect_keyset(
        "events",
        page_limit=1,
        max_pages=0,
        resume=True,
    )

    assert result.pages == 2
    assert len(raw_writer.records) == 2
