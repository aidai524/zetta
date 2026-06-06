from zetta.collectors.data import DataCollector
from zetta.http import HttpClientError, HttpResponse
from zetta.polymarket import Page
from zetta.storage.state import LocalStateStore


class FakeRawWriter:
    def __init__(self) -> None:
        self.records = []

    def write(self, **record):
        self.records.append(record)
        return "raw.jsonl.gz"


class PagedTradesClient:
    def __init__(self) -> None:
        self.calls = 0

    def data_trades(self, **kwargs):
        self.calls += 1
        items = [{"id": "1"}, {"id": "2"}] if self.calls == 1 else [{"id": "3"}]
        return Page(
            response=HttpResponse(
                url=f"https://example.test/trades?offset={kwargs['offset']}",
                status=200,
                headers={},
                body=items,
            ),
            items=items,
        )


class OffsetLimitedTradesClient:
    def __init__(self) -> None:
        self.calls = 0

    def data_trades(self, **kwargs):
        self.calls += 1
        if kwargs["offset"] >= 2:
            raise HttpClientError(
                "GET https://example.test/trades failed with 400: "
                '{"error":"max historical activity offset of 3000 exceeded"}'
            )
        items = [{"id": "1"}, {"id": "2"}]
        return Page(
            response=HttpResponse(
                url=f"https://example.test/trades?offset={kwargs['offset']}",
                status=200,
                headers={},
                body=items,
            ),
            items=items,
        )


def test_trades_collector_max_pages_zero_runs_until_short_page(tmp_path) -> None:
    raw_writer = FakeRawWriter()
    client = PagedTradesClient()
    collector = DataCollector(
        client=client,
        raw_writer=raw_writer,
        state_store=LocalStateStore(tmp_path),
    )

    result = collector.collect_trades(page_limit=2, max_pages=0, resume=False, market="m1")

    assert result.pages == 2
    assert result.trades == 3
    assert client.calls == 2
    assert len(raw_writer.records) == 2


def test_trades_collector_stops_at_data_api_offset_limit(tmp_path) -> None:
    raw_writer = FakeRawWriter()
    client = OffsetLimitedTradesClient()
    collector = DataCollector(
        client=client,
        raw_writer=raw_writer,
        state_store=LocalStateStore(tmp_path),
    )

    result = collector.collect_trades(page_limit=2, max_pages=0, resume=False, market="m1")

    assert result.pages == 1
    assert result.trades == 2
    assert result.next_offset == 2
    assert client.calls == 2
    assert len(raw_writer.records) == 1
