from argparse import Namespace
from dataclasses import asdict
from types import SimpleNamespace

from zetta.cli import cmd_refresh_live_token_metadata, parse_token_ids
from zetta.config import Settings
from zetta.loaders.gamma import GammaLoadResult
from zetta.polymarket import PolymarketClient


def test_parse_token_ids_accepts_repeated_and_comma_separated_values() -> None:
    assert parse_token_ids(["token-1, token-2", "token-3"]) == [
        "token-1",
        "token-2",
        "token-3",
    ]


def test_polymarket_client_queries_gamma_market_by_clob_token(monkeypatch) -> None:
    captured = {}

    class FakeHttp:
        def __init__(self, **_kwargs) -> None:
            pass

        def get(self, url, params):
            captured["url"] = url
            captured["params"] = params
            return SimpleNamespace(url="https://gamma.test/markets?clob_token_ids=token-1", body=[{"id": "m1"}])

    monkeypatch.setattr("zetta.polymarket.JsonHttpClient", FakeHttp)

    page = PolymarketClient(Settings(gamma_base_url="https://gamma.test")).gamma_markets_by_clob_token_id(
        token_id="token-1",
        limit=7,
    )

    assert captured["url"] == "https://gamma.test/markets"
    assert captured["params"] == {"limit": 7, "clob_token_ids": "token-1"}
    assert page.items == [{"id": "m1"}]


def test_refresh_live_token_metadata_fetches_and_loads_missing_tokens(monkeypatch, tmp_path) -> None:
    captured = {"queries": [], "requests": [], "writes": [], "loads": []}

    class FakeClickHouse:
        def __init__(self, _settings) -> None:
            pass

        def query_text(self, query):
            captured["queries"].append(query)
            return '{"token_id":"token-1"}\n{"token_id":"token-2"}\n'

    class FakeClient:
        def __init__(self, _settings) -> None:
            pass

        def gamma_markets_by_clob_token_id(self, *, token_id):
            captured["requests"].append(token_id)
            return SimpleNamespace(
                response=SimpleNamespace(
                    url=f"https://gamma.test/markets?clob_token_ids={token_id}",
                    body=[{"id": f"market-{token_id}", "clobTokenIds": f'["{token_id}"]'}],
                ),
                items=[{"id": f"market-{token_id}"}],
            )

    class FakeWriter:
        def write(self, *, source, entity, request_url, payload):
            captured["writes"].append(
                {
                    "source": source,
                    "entity": entity,
                    "request_url": request_url,
                    "payload": payload,
                }
            )
            return tmp_path / f"{len(captured['writes'])}.jsonl.gz"

        def flush(self):
            return []

    class FakeGammaLoader:
        def __init__(self, *, clickhouse) -> None:
            self.clickhouse = clickhouse

        def load(self, *, raw_root, batch_size):
            captured["loads"].append({"raw_root": raw_root, "batch_size": batch_size})
            return GammaLoadResult(
                raw_records=2,
                skipped_raw_records=0,
                events=0,
                markets=2,
                outcome_tokens=2,
                series=0,
                tags=0,
                event_markets=0,
                event_series=0,
                event_tags=0,
                ingest_logs=2,
            )

    monkeypatch.setattr("zetta.cli.ClickHouseWriter", FakeClickHouse)
    monkeypatch.setattr("zetta.cli.PolymarketClient", FakeClient)
    monkeypatch.setattr("zetta.cli.raw_writer", lambda _settings: FakeWriter())
    monkeypatch.setattr("zetta.cli.GammaRawLoader", FakeGammaLoader)

    result = cmd_refresh_live_token_metadata(
        Namespace(
            lookback_minutes=10,
            limit=20,
            min_notional=100.0,
            sleep_seconds=0.0,
            load_batch_size=123,
        ),
        Settings(raw_data_dir=tmp_path),
    )

    assert captured["requests"] == ["token-1", "token-2"]
    assert [write["entity"] for write in captured["writes"]] == ["markets", "markets"]
    assert captured["loads"] == [{"raw_root": tmp_path, "batch_size": 123}]
    assert result["candidate_token_ids"] == 2
    assert result["markets"] == 2
    assert result["load"] == asdict(
        GammaLoadResult(
            raw_records=2,
            skipped_raw_records=0,
            events=0,
            markets=2,
            outcome_tokens=2,
            series=0,
            tags=0,
            event_markets=0,
            event_series=0,
            event_tags=0,
            ingest_logs=2,
        )
    )
    assert "fact_exchange_fill" in captured["queries"][0]
    assert "fills.notional >= 100.0" in captured["queries"][0]
