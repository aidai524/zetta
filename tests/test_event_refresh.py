from __future__ import annotations

import json

from zetta.config import Settings
from zetta.http import HttpResponse
from zetta.polymarket import Page
from zetta.scheduler.runner import TaskRunner
from zetta.scheduler.tasks import LocalRunStore, LocalTaskStore, Task


class FakeRawWriter:
    def __init__(self) -> None:
        self.records = []

    def write(self, *, source, entity, request_url, payload):
        path = f"/raw/{source}/{entity}/{len(self.records)}.jsonl.gz"
        self.records.append(
            {
                "source": source,
                "entity": entity,
                "request_url": request_url,
                "payload": payload,
                "path": path,
            }
        )
        return path

    def flush(self):
        return []


class FakeClient:
    def data_trades(self, **kwargs):
        items = [{"conditionId": kwargs["market"], "timestamp": "2026-01-01T00:00:00Z"}]
        return page(f"https://data.test/trades?market={kwargs['market']}", items)

    def data_holders(self, *, market, limit):
        return page(f"https://data.test/holders?market={market}", [{"conditionId": market}])

    def data_market_positions(self, *, market, limit):
        return page(f"https://data.test/positions?market={market}", [{"conditionId": market}])

    def data_open_interest(self, *, market):
        return page(f"https://data.test/oi?market={market}", [{"conditionId": market}])

    def clob_prices_history(self, *, market, start_ts=None, end_ts=None, interval=None, fidelity=None):
        return page(f"https://clob.test/prices-history?market={market}", [{"t": 1, "p": 0.5}])

    def clob_book(self, *, token_id):
        return page(f"https://clob.test/book?token_id={token_id}", [{"asset_id": token_id}])


class FakeClickHouse:
    def __init__(self, _settings):
        pass

    def query_text(self, _query):
        return "\n".join(
            [
                json.dumps(
                    {
                        "market_id": "market-1",
                        "condition_id": "condition-1",
                        "token_ids": ["token-1", "token-2"],
                    }
                )
            ]
        )


def page(url: str, items: list[dict]) -> Page:
    return Page(
        response=HttpResponse(url=url, status=200, headers={}, body=items),
        items=items,
    )


def test_event_refresh_collects_all_event_scoped_entities(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("zetta.scheduler.runner.ClickHouseWriter", FakeClickHouse)
    store = LocalTaskStore(tmp_path / "tasks.json")
    task = Task(
        kind="event-refresh",
        params={
            "event_id": "event-1",
            "refresh_run": "2026-06-08T00:00:00+00:00",
            "trade_page_limit": 500,
            "trade_max_pages": 1,
        },
    )
    store.add_many([task])
    runner = TaskRunner(
        settings=Settings(raw_data_dir=tmp_path / "raw", state_dir=tmp_path / "state"),
        task_store=store,
        run_store=LocalRunStore(tmp_path / "runs.jsonl"),
    )
    runner.client = FakeClient()
    runner.raw_writer = FakeRawWriter()

    result = runner.run_once()

    assert result["status"] == "done"
    refresh = result["result"]
    assert refresh["event_id"] == "event-1"
    assert refresh["markets"] == 1
    assert refresh["condition_ids"] == 1
    assert refresh["token_ids"] == 2
    assert refresh["failures"] == []
    assert {record["entity"] for record in runner.raw_writer.records} == {
        "trades",
        "holders",
        "market_positions",
        "open_interest",
        "prices_history",
        "book",
    }
