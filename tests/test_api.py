import json
import re

from zetta.api import ProductApi, ch_string, collect_system_stats, int_param, rows_json
from zetta.config import Settings


class FakeClickHouse:
    def __init__(self, output="", outputs=None) -> None:
        self.output = output
        self.outputs = list(outputs or [])
        self.queries = []

    def query_text(self, query):
        self.queries.append(query)
        if self.outputs:
            return self.outputs.pop(0)
        return self.output


def test_rows_json_parses_jsoneachrow() -> None:
    assert rows_json('{"a":1}\n{"b":"two"}\n') == [{"a": 1}, {"b": "two"}]


def test_query_helpers_escape_and_bound_values() -> None:
    assert ch_string("can't") == "'can\\'t'"
    assert int_param({"limit": ["999"]}, "limit", 10, maximum=100) == 100
    assert int_param({"limit": ["bad"]}, "limit", 10, maximum=100) == 10


def test_product_api_market_search_returns_rows() -> None:
    fake = FakeClickHouse('{"market_id":"m1","question":"Will it work?"}\n')
    api = ProductApi(clickhouse=fake)

    response = api.handle("/markets/search", {"q": ["work"], "limit": ["1"]})

    assert response.status == 200
    assert response.body["markets"][0]["market_id"] == "m1"
    assert "positionCaseInsensitive" in fake.queries[0]


def test_product_api_trader_profile_not_found() -> None:
    api = ProductApi(clickhouse=FakeClickHouse(outputs=["", "", "", "", ""]))

    response = api.handle("/traders/profile", {"user": ["0xabc"]})

    assert response.status == 404
    assert response.body == {"error": "trader_not_found"}


def test_product_api_trader_profile_can_use_wallet_rollup() -> None:
    fake = FakeClickHouse(
        outputs=[
            '{"user_address":"0xabc","trade_count":14,"buy_count":12,"sell_count":2,'
            '"traded_size":10,"traded_notional":5000,"position_count":0,"current_value":0,'
            '"cash_pnl":0,"realized_pnl":0,"total_pnl":0,"chain_fill_count":0,'
            '"chain_traded_size":0,"chain_traded_notional":0,"chain_position_size":0,'
            '"chain_current_value":0,"chain_net_cashflow":0,"chain_mark_to_market_pnl":0,'
            '"first_trade_at":"2026-06-11 18:00:00.000",'
            '"last_trade_at":"2026-06-11 21:00:00.000","last_position_at":null,'
            '"last_chain_fill_block":0,"trade_count_24h":14,'
            '"traded_notional_24h":5000,"latest_action":"BUY","data_lag_seconds":120}\n',
            "",
            "",
            "",
            "",
        ]
    )
    api = ProductApi(clickhouse=fake)

    response = api.handle("/traders/profile", {"user": ["0xABC"]})

    assert response.status == 200
    assert response.body["profile"]["trade_count"] == 14
    assert response.body["profile"]["trade_count_24h"] == 14
    assert "mart_wallet_trade_rollup" in fake.queries[0]


def test_product_api_trader_profile_falls_back_to_trade_by_user() -> None:
    fake = FakeClickHouse(
        outputs=[
            "",
            '{"user_address":"0xabc","trade_count":6,"buy_count":4,"sell_count":2,'
            '"traded_size":12,"traded_notional":830.5,"position_count":3,'
            '"current_value":220.25,"cash_pnl":0,"realized_pnl":0,"total_pnl":17.75,'
            '"chain_fill_count":0,"chain_traded_size":0,"chain_traded_notional":0,'
            '"chain_position_size":0,"chain_current_value":0,"chain_net_cashflow":0,'
            '"chain_mark_to_market_pnl":0,"first_trade_at":"2026-06-11 18:00:00.000",'
            '"last_trade_at":"2026-06-12 03:00:00.000",'
            '"last_position_at":"2026-06-12 03:00:00.000","last_chain_fill_block":0,'
            '"trade_count_24h":6,"traded_notional_24h":830.5,'
            '"buy_notional_24h":600.25,"sell_notional_24h":230.25,'
            '"latest_action":"SELL","data_lag_seconds":60}\n',
            "",
            "",
        ]
    )
    api = ProductApi(clickhouse=fake)

    response = api.handle("/traders/profile", {"user": ["0xABC"]})

    assert response.status == 200
    profile = response.body["profile"]
    assert profile["trade_count"] == 6
    assert profile["position_count"] == 3
    assert profile["current_value"] == 220.25
    assert profile["total_pnl"] == 17.75
    assert "fact_trade_by_user" in fake.queries[1]
    assert "mart_trader_chain_pnl" in fake.queries[2]


def test_product_api_trader_profile_prefers_wallet_activity_positions() -> None:
    fake = FakeClickHouse(
        outputs=[
            '{"user_address":"0xabc","trade_count":15,"buy_count":13,"sell_count":2,'
            '"traded_size":10,"traded_notional":5516.76,"position_count":0,'
            '"current_value":0,"cash_pnl":0,"realized_pnl":0,"total_pnl":0,'
            '"chain_fill_count":0,"chain_traded_size":0,"chain_traded_notional":0,'
            '"chain_position_size":0,"chain_current_value":0,"chain_net_cashflow":0,'
            '"chain_mark_to_market_pnl":0,"first_trade_at":"2026-06-03 04:22:03.000",'
            '"last_trade_at":"2026-06-11 21:24:03.000","last_position_at":null,'
            '"last_chain_fill_block":0,"trade_count_24h":13,'
            '"traded_notional_24h":5006.76,"buy_notional_24h":4270.3,'
            '"sell_notional_24h":736.46,"latest_action":"BUY","data_lag_seconds":3600}\n',
            '{"user_address":"0xabc","trade_count":20,"buy_count":16,"sell_count":4,'
            '"traded_size":100,"traded_notional":7725.24,"position_count":10,'
            '"current_value":3301.86,"cash_pnl":0,"realized_pnl":0,"total_pnl":345.62,'
            '"chain_fill_count":0,"chain_traded_size":0,"chain_traded_notional":0,'
            '"chain_position_size":0,"chain_current_value":0,"chain_net_cashflow":0,'
            '"chain_mark_to_market_pnl":0,"first_trade_at":"2026-05-30 16:06:35.000",'
            '"last_trade_at":"2026-06-11 21:24:03.000",'
            '"last_position_at":"2026-06-11 21:24:03.000","last_chain_fill_block":0,'
            '"trade_count_24h":14,"traded_notional_24h":6139.34,'
            '"buy_notional_24h":5000,"sell_notional_24h":1139.34,'
            '"latest_action":"BUY","data_lag_seconds":120}\n',
            '{"user_address":"0xabc","chain_fill_count":56,'
            '"chain_traded_size":262235.05,"chain_traded_notional":15715.63,'
            '"chain_position_size":64528.02,"chain_current_value":482.5,'
            '"chain_net_cashflow":2705.46,"chain_mark_to_market_pnl":3187.96,'
            '"last_chain_fill_block":88338705}\n',
            "",
        ]
    )
    api = ProductApi(clickhouse=fake)

    response = api.handle("/traders/profile", {"user": ["0xABC"]})

    assert response.status == 200
    profile = response.body["profile"]
    assert profile["trade_count"] == 20
    assert profile["position_count"] == 10
    assert profile["current_value"] == 3301.86
    assert profile["total_pnl"] == 345.62
    assert profile["chain_fill_count"] == 56
    assert profile["chain_traded_notional"] == 15715.63
    assert profile["first_trade_at"] == "2026-05-30 16:06:35.000"
    assert "fact_user_activity" in fake.queries[1]
    assert "mart_trader_chain_pnl" in fake.queries[2]


def test_product_api_trader_profile_prefers_portfolio_snapshot() -> None:
    fake = FakeClickHouse(
        outputs=[
            "",
            '{"user_address":"0xabc","trade_count":31,"buy_count":20,"sell_count":11,'
            '"traded_size":100,"traded_notional":817.8,"position_count":18,'
            '"current_value":526.51,"cash_pnl":0,"realized_pnl":0,"total_pnl":398.36,'
            '"chain_fill_count":0,"chain_traded_size":0,"chain_traded_notional":0,'
            '"chain_position_size":0,"chain_current_value":0,"chain_net_cashflow":0,'
            '"chain_mark_to_market_pnl":0,"first_trade_at":"2026-05-30 16:06:35.000",'
            '"last_trade_at":"2026-06-12 03:47:16.000",'
            '"last_position_at":"2026-06-12 03:47:16.000","last_chain_fill_block":0,'
            '"trade_count_24h":31,"traded_notional_24h":817.8,'
            '"buy_notional_24h":600,"sell_notional_24h":217.8,'
            '"latest_action":"BUY","data_lag_seconds":60}\n',
            "",
            '{"user_address":"0xabc","position_count":4,"positions_value":24.2423,'
            '"portfolio_value":25.444569,"available_balance":1.202269,'
            '"total_pnl":154.27983,"last_position_at":"2026-06-12 09:00:00.000"}\n',
        ]
    )
    api = ProductApi(clickhouse=fake)

    response = api.handle("/traders/profile", {"user": ["0xABC"]})

    assert response.status == 200
    profile = response.body["profile"]
    assert profile["position_count"] == 4
    assert profile["current_value"] == 24.2423
    assert profile["portfolio_value"] == 25.444569
    assert profile["available_balance"] == 1.202269
    assert profile["total_pnl"] == 154.27983


def test_product_api_wallet_screener_uses_all_wallet_screener_mart() -> None:
    fake = FakeClickHouse(
        '{"user_address":"0xabc","traded_notional":1500000,'
        '"max_single_trade_notional":110000,"is_whale":true}\n'
    )
    api = ProductApi(clickhouse=fake)

    response = api.handle("/wallets/screener", {"mode": ["whale"], "limit": ["5"]})

    assert response.status == 200
    assert response.body["wallets"][0]["user_address"] == "0xabc"
    query = fake.queries[0]
    assert "mart_wallet_screener" in query
    assert "max_single_trade_notional >= 100000.0" in query
    assert "traded_notional >= 1000000.0" in query


def test_product_api_wallet_screener_smart_mode_uses_roi_definition() -> None:
    fake = FakeClickHouse("")
    api = ProductApi(clickhouse=fake)

    response = api.handle("/wallets/screener", {"mode": ["smart"], "limit": ["5"]})

    assert response.status == 200
    query = fake.queries[0]
    assert "screener.traded_notional >= 10000.0" in query
    assert "screener.pnl_roi >= 0.55" in query
    assert "screener.pnl_roi desc" in query


def test_product_api_wallet_detail_returns_portfolio_pnl_and_activity() -> None:
    portfolio_raw = {
        "positions": [
            {
                "asset": "asset-1",
                "conditionId": "c1",
                "title": "Spread: Germany (-3.5)",
                "slug": "fifwc-ger-kor-2026-06-14-spread-home-3pt5",
                "eventSlug": "fifwc-ger-kor-2026-06-14-more-markets",
                "outcome": "Germany",
                "size": 100,
                "avgPrice": 0.5,
                "curPrice": 0.8,
                "initialValue": 50,
                "currentValue": 80,
                "cashPnl": 30,
                "percentPnl": 60,
                "redeemable": False,
            },
            {
                "asset": "asset-2",
                "conditionId": "c2",
                "title": "Old market",
                "slug": "mlb-old",
                "eventSlug": "mlb-old",
                "outcome": "Team",
                "size": 10,
                "avgPrice": 0.4,
                "curPrice": 0,
                "initialValue": 4,
                "currentValue": 0,
                "cashPnl": -4,
                "percentPnl": -100,
                "redeemable": True,
            },
        ]
    }
    pnl_raw = {"points": [{"t": 1781481600, "p": 1000.5}, {"t": 1781568000, "p": 1200.25}]}
    fake = FakeClickHouse(
        outputs=[
            (
                '{"user_address":"0xabc","captured_at":"2026-06-14 18:51:07.344",'
                '"position_count":2,"positions_value":80,"portfolio_value":85,'
                '"available_balance":5,"total_pnl":900,'
                f'"raw_json":{json.dumps(json.dumps(portfolio_raw))}}}\n'
            ),
            (
                '{"user_address":"0xabc","captured_at":"2026-06-15 07:36:07.032",'
                '"total_pnl":1200.25,'
                f'"raw_json":{json.dumps(json.dumps(pnl_raw))}}}\n'
            ),
            (
                '{"user_address":"0xabc","activity_count":10,"trade_activity_count":8,'
                '"buy_count":6,"sell_count":2,"traded_size":1000,'
                '"traded_notional":500,"buy_notional":350,"sell_notional":150,'
                '"activity_count_24h":3,"trade_activity_count_24h":2,'
                '"traded_notional_24h":100,"trade_activity_count_7d":7,'
                '"traded_notional_7d":420,"avg_bet":62.5,'
                '"latest_activity_type":"TRADE",'
                '"latest_side":"BUY","first_activity_at":"2026-06-11 00:00:00.000",'
                '"last_activity_at":"2026-06-15 03:03:13.000"}\n'
            ),
            '{"activity_type":"TRADE","count":8,"notional":500}\n',
            (
                '{"timestamp":"2026-06-15 03:03:13.000","activity_type":"TRADE",'
                '"side":"BUY","price":0.52,"size":10,"notional":5.2,'
                '"condition_id":"c1","token_id":"asset-1","transaction_hash":"0xhash",'
                '"title":"Spread: Germany (-3.5)","slug":"fifwc-ger-kor-2026-06-14-spread-home-3pt5",'
                '"event_slug":"fifwc-ger-kor-2026-06-14-more-markets","outcome":"Germany"}\n'
            ),
            (
                '{"user_address":"0xabc","completed_event_count":10,'
                '"profitable_event_count":6,"losing_event_count":4,"win_rate":0.6,'
                '"realized_pnl":150,"avg_event_roi":0.12,"best_event_pnl":50,'
                '"worst_event_pnl":-30,"active_position_count":2,"active_event_count":1,'
                '"active_unrealized_pnl_estimate":80,"favorite_category":"sports",'
                '"favorite_category_notional":400,"first_trade_at":"2026-06-11 00:00:00.000",'
                '"last_trade_at":"2026-06-15 03:03:13.000"}\n'
            ),
        ]
    )
    api = ProductApi(clickhouse=fake)

    response = api.handle(
        "/wallets/detail",
        {
            "user": ["0xABC"],
            "position_scope": ["worldcup"],
            "position_limit": ["5"],
            "pnl_points_limit": ["1"],
        },
    )

    assert response.status == 200
    body = response.body
    assert body["wallet"]["user_address"] == "0xabc"
    assert body["wallet"]["latest_total_pnl"] == 1200.25
    assert body["wallet"]["portfolio_value"] == 85.0
    assert body["wallet"]["cash"] == 5.0
    assert body["wallet"]["pnl_7d"] == 0.0
    assert body["wallet"]["trade_volume_7d"] == 420.0
    assert body["wallet"]["trade_count_7d"] == 7
    assert body["wallet"]["avg_bet"] == 62.5
    assert body["wallet"]["win_rate"] == 0.6
    assert body["position_summary"]["position_count"] == 2
    assert body["position_summary"]["worldcup_position_count"] == 1
    assert body["positions_available"] == 1
    assert body["positions"][0]["slug"] == "fifwc-ger-kor-2026-06-14-spread-home-3pt5"
    assert body["positions"][0]["is_worldcup"] is True
    assert body["pnl_point_count"] == 1
    assert body["pnl_points"][0]["pnl"] == 1200.25
    assert body["activity_summary"]["trade_activity_count"] == 8
    assert body["activity_by_type"][0]["activity_type"] == "TRADE"
    assert body["reputation"]["completed_event_count"] == 10
    assert body["recent_activity"][0]["title"] == "Spread: Germany (-3.5)"
    assert "from fact_wallet_portfolio_snapshot" in fake.queries[0]
    assert "from fact_wallet_pnl_snapshot" in fake.queries[1]
    assert "from fact_user_activity" in fake.queries[2]


def test_product_api_wallet_detail_not_found() -> None:
    fake = FakeClickHouse(outputs=["", "", "", "", "", ""])
    api = ProductApi(clickhouse=fake)

    response = api.handle("/wallets/detail", {"user": ["0xmissing"]})

    assert response.status == 404
    assert response.body == {"error": "wallet_not_found"}


def test_product_api_wallet_detail_refresh_enqueues_high_priority_tasks(monkeypatch) -> None:
    captured = {}

    class FakeTaskStore:
        def __init__(self, **kwargs) -> None:
            captured["kwargs"] = kwargs

        def add_many(self, tasks):
            captured["tasks"] = tasks
            return len(tasks)

    fake = FakeClickHouse(outputs=["", "", "", "", "", ""])
    monkeypatch.setattr("zetta.api.PostgresTaskStore", FakeTaskStore)
    api = ProductApi(clickhouse=fake, settings=Settings(postgres_dsn="postgresql://example"))

    response = api.handle("/wallets/detail", {"user": ["0xABC"], "refresh": ["1"]})

    assert response.status == 404
    assert response.body == {"error": "wallet_not_found"}
    assert captured["kwargs"]["dsn"] == "postgresql://example"
    tasks = captured["tasks"]
    assert {task.kind for task in tasks} == {
        "wallet-activity",
        "wallet-pnl",
        "wallet-portfolio",
        "wallet-trades",
    }
    assert {task.priority for task in tasks} == {-2}
    assert {task.params["user"] for task in tasks} == {"0xabc"}
    assert all(task.params["_requeue_done"] is True for task in tasks)


def test_product_api_tasks_nodes_returns_node_progress(monkeypatch) -> None:
    calls = {"count": 0}

    class FakeTaskStore:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs

        def node_progress(self, *, lookback_minutes):
            calls["count"] += 1
            return {
                "lookback_minutes": lookback_minutes,
                "nodes": [{"node_id": "wallet-helper-1-1", "role": "wallet-helper"}],
                "totals": {"nodes": 1},
            }

    monkeypatch.setattr("zetta.api.PostgresTaskStore", FakeTaskStore)
    api = ProductApi(clickhouse=FakeClickHouse(), settings=Settings(postgres_dsn="postgresql://example"))

    response = api.handle("/tasks/nodes", {"lookback_minutes": ["15"]})
    cached_response = api.handle("/tasks/nodes", {"lookback_minutes": ["15"]})

    assert response.status == 200
    assert response.body["lookback_minutes"] == 15
    assert response.body["nodes"][0]["node_id"] == "wallet-helper-1-1"
    assert cached_response.body == response.body
    assert calls["count"] == 1


def test_product_api_tasks_progress_uses_short_cache(monkeypatch) -> None:
    calls = {"count": 0}

    class FakeTaskStore:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs

        def progress(self, *, recent_limit):
            calls["count"] += 1
            return {"recent_limit": recent_limit, "summary": {"done": calls["count"]}}

    monkeypatch.setattr("zetta.api.PostgresTaskStore", FakeTaskStore)
    api = ProductApi(clickhouse=FakeClickHouse(), settings=Settings(postgres_dsn="postgresql://example"))

    response = api.handle("/tasks/progress", {"recent_limit": ["8"]})
    cached_response = api.handle("/tasks/progress", {"recent_limit": ["8"]})

    assert response.status == 200
    assert response.body["recent_limit"] == 8
    assert cached_response.body == response.body
    assert calls["count"] == 1


def test_product_api_worldcup_wallet_rankings_defaults_to_compact_response() -> None:
    fake = FakeClickHouse(outputs=worldcup_positive_wallet_outputs())
    api = ProductApi(clickhouse=fake)

    response = api.handle("/worldcup/wallet-rankings", {"slug": ["fifwc-can-bih-2026-06-12"]})

    assert response.status == 200
    body = response.body
    assert body["data_status"] == "ok"
    assert body["scope"]["input_slug_count"] == 4
    assert body["cumulative_profit_wallets"][0]["user_address"] == "0xabc"
    assert body["cumulative_profit_wallets"][0]["rank_metric"]["pnl"] == 50.0
    assert "matches" not in body["cumulative_profit_wallets"][0]


def test_product_api_worldcup_wallet_rankings_can_include_details() -> None:
    fake = FakeClickHouse(outputs=worldcup_positive_wallet_outputs())
    api = ProductApi(clickhouse=fake)

    response = api.handle(
        "/worldcup/wallet-rankings",
        {"slug": ["fifwc-can-bih-2026-06-12"], "details": ["true"]},
    )

    assert response.status == 200
    first = response.body["cumulative_profit_wallets"][0]
    assert first["matches"][0]["match_slug"] == "fifwc-can-bih-2026-06-12"
    assert first["matches"][0]["tokens"][0]["mark_price_source"] == "price_history"


def test_product_api_worldcup_wallet_rankings_supports_exact_slug_mode() -> None:
    fake = FakeClickHouse(outputs=worldcup_positive_wallet_outputs())
    api = ProductApi(clickhouse=fake)

    response = api.handle(
        "/worldcup/wallet-rankings",
        {
            "slug": ["fifwc-can-bih-2026-06-12"],
            "expand_variants": ["false"],
        },
    )

    assert response.status == 200
    assert response.body["scope"]["input_slugs"] == ["fifwc-can-bih-2026-06-12"]


def test_product_api_worldcup_wallet_rankings_uses_short_cache() -> None:
    fake = FakeClickHouse(outputs=worldcup_positive_wallet_outputs())
    api = ProductApi(clickhouse=fake)

    first = api.handle("/worldcup/wallet-rankings", {"slug": ["fifwc-can-bih-2026-06-12"]})
    second = api.handle("/worldcup/wallet-rankings", {"slug": ["fifwc-can-bih-2026-06-12"]})

    assert first.status == 200
    assert second.status == 200
    assert len(fake.queries) == 4
    assert second.body["cumulative_profit_wallets"][0]["user_address"] == "0xabc"


def worldcup_positive_wallet_outputs() -> list[str]:
    return [
        (
            '{"event_id":"e1","event_slug":"fifwc-can-bih-2026-06-12",'
            '"event_title":"Canada vs Bosnia and Herzegovina","market_id":"m1",'
            '"condition_id":"c1","market_question":"Canada vs Bosnia and Herzegovina",'
            '"token_id":"t1","outcome":"Canada"}\n'
        ),
        (
            '{"condition_id":"c1","token_id":"t1","user_address":"0xabc",'
            '"trade_count":1,"buy_count":1,"sell_count":0,"buy_size":200,'
            '"sell_size":0,"buy_notional":100,"sell_notional":0,'
            '"traded_notional":100,"last_trade_price":0.5,'
            '"first_trade_at":"2026-06-12 12:00:00.000",'
            '"last_trade_at":"2026-06-12 12:00:00.000"}\n'
        ),
        (
            '{"token_id":"t1","last_trade_at":"2026-06-12 12:00:00.000",'
            '"last_trade_price":0.5}\n'
        ),
        (
            '{"token_id":"t1","book_best_bid":null,"book_best_ask":null,'
            '"book_mark_at":null,"price_history_price":0.75,'
            '"price_history_at":"2026-06-12 12:01:00.000"}\n'
        ),
    ]


def test_product_api_wallet_summary_counts_screened_wallets() -> None:
    fake = FakeClickHouse(
        '{"total_wallets":2191819,"wallets_over_10k":94792,'
        '"smart_wallets":3219,"whale_wallets":2242}\n'
    )
    api = ProductApi(clickhouse=fake)

    response = api.handle("/wallets/summary", {})

    assert response.status == 200
    assert response.body["summary"]["total_wallets"] == 2191819
    assert response.body["summary"]["smart_wallets"] == 3219
    query = fake.queries[0]
    assert "mart_wallet_screener final" in query
    assert "traded_notional >= 10000.0" in query
    assert "pnl_roi >= 0.55" in query
    assert "max_single_trade_notional >= 100000.0" in query


def test_product_api_event_smart_wallets_returns_compact_options() -> None:
    fake = FakeClickHouse(
        outputs=[
            '{"event_id":"351717","slug":"fifwc-can-bih-2026-06-12",'
            '"title":"Canada vs. Bosnia and Herzegovina"}\n',
            '{"market_question":"Will Canada win?","token_outcome":"Yes",'
            '"outcome_side":"YES","selection":"Will Canada win? / YES",'
            '"smart_wallet_count":"1","smart_amount":1000,'
            '"whale_wallet_count":"2","whale_amount":2500}\n'
            '{"market_question":"Will Canada win?","token_outcome":"No",'
            '"outcome_side":"NO","selection":"Will Canada win? / NO",'
            '"smart_wallet_count":"0","smart_amount":0,'
            '"whale_wallet_count":"0","whale_amount":0}\n',
        ]
    )
    api = ProductApi(clickhouse=fake)

    response = api.handle(
        "/events/smart-wallets",
        {"event": ["fifwc-can-bih-2026-06-12"], "limit": ["5"]},
    )

    assert response.status == 200
    assert response.body["event"]["event_id"] == "351717"
    assert response.body["data_status"] == "ok"
    assert response.body["options"] == [
        {
            "market_question": "Will Canada win?",
            "yes": {
                "smart_wallet_count": 1,
                "smart_amount": 1000.0,
                "whale_wallet_count": 2,
                "whale_amount": 2500.0,
            },
            "no": {
                "smart_wallet_count": 0,
                "smart_amount": 0.0,
                "whale_wallet_count": 0,
                "whale_amount": 0.0,
            },
        }
    ]
    assert "left join option_stats" in fake.queries[1]


def test_product_api_event_smart_wallet_options_is_always_compact() -> None:
    fake = FakeClickHouse(
        outputs=[
            '{"event_id":"351717","slug":"fifwc-can-bih-2026-06-12",'
            '"title":"Canada vs. Bosnia and Herzegovina"}\n',
            '{"market_question":"Will Canada win?","token_outcome":"No",'
            '"outcome_side":"NO","selection":"Will Canada win? / NO",'
            '"smart_wallet_count":0,"smart_amount":0,'
            '"whale_wallet_count":0,"whale_amount":0}\n',
        ]
    )
    api = ProductApi(clickhouse=fake)

    response = api.handle(
        "/events/smart-wallet-options",
        {"event": ["fifwc-can-bih-2026-06-12"], "details": ["1"]},
    )

    assert response.status == 200
    assert sorted(response.body.keys()) == ["data_status", "event", "message", "options"]
    assert response.body["data_status"] == "ok"
    assert response.body["options"] == [
        {
            "market_question": "Will Canada win?",
            "yes": {
                "smart_wallet_count": 0,
                "smart_amount": 0.0,
                "whale_wallet_count": 0,
                "whale_amount": 0.0,
            },
            "no": {
                "smart_wallet_count": 0,
                "smart_amount": 0.0,
                "whale_wallet_count": 0,
                "whale_amount": 0.0,
            },
        }
    ]


def test_product_api_event_smart_wallet_options_not_found_is_empty_response() -> None:
    fake = FakeClickHouse(outputs=[""])
    api = ProductApi(clickhouse=fake)

    response = api.handle(
        "/events/smart-wallet-options",
        {"event": ["fifwc-missing-2026-06-12"]},
    )

    assert response.status == 200
    assert response.body["event"]["slug"] == "fifwc-missing-2026-06-12"
    assert response.body["options"] == []
    assert response.body["data_status"] == "event_not_found"
    assert response.body["message"] == "No data is available for this event yet."


def test_product_api_event_smart_wallet_options_no_markets_is_empty_response() -> None:
    fake = FakeClickHouse(
        outputs=[
            '{"event_id":"351717","slug":"fifwc-can-bih-2026-06-12",'
            '"title":"Canada vs. Bosnia and Herzegovina"}\n',
            "",
        ]
    )
    api = ProductApi(clickhouse=fake)

    response = api.handle(
        "/events/smart-wallet-options",
        {"event": ["fifwc-can-bih-2026-06-12"]},
    )

    assert response.status == 200
    assert response.body["event"]["event_id"] == "351717"
    assert response.body["options"] == []
    assert response.body["data_status"] == "no_options"
    assert response.body["message"] == "No option data is available for this event yet."


def test_product_api_event_smart_wallets_details_returns_wallet_rows() -> None:
    fake = FakeClickHouse(
        outputs=[
            '{"event_id":"351717","slug":"fifwc-can-bih-2026-06-12",'
            '"title":"Canada vs. Bosnia and Herzegovina"}\n',
            '{"event_slug":"fifwc-can-bih-2026-06-12","event_title":"Canada vs. Bosnia and Herzegovina",'
            '"smart_trade_count":2,"smart_wallet_count":2,"smart_traded_notional":1100.24}\n',
            '{"event_slug":"fifwc-can-bih-2026-06-12","market_question":"Will Canada win?",'
            '"token_outcome":"Yes","outcome_side":"YES","selection":"Will Canada win? / YES",'
            '"smart_trade_count":1,"smart_wallet_count":1}\n',
            '{"event_slug":"fifwc-can-bih-2026-06-12","market_question":"Will Canada win?",'
            '"token_outcome":"Yes","outcome_side":"YES","selection":"Will Canada win? / YES",'
            '"user_address":"0xabc","event_traded_notional":1000}\n',
            "",
        ]
    )
    api = ProductApi(clickhouse=fake)

    response = api.handle(
        "/events/smart-wallets",
        {"event": ["fifwc-can-bih-2026-06-12"], "limit": ["5"], "details": ["1"]},
    )

    assert response.status == 200
    assert response.body["event"]["event_id"] == "351717"
    assert response.body["summary"]["smart_wallet_count"] == 2
    assert response.body["outcomes"][0]["outcome_side"] == "YES"
    assert response.body["outcomes"][0]["selection"] == "Will Canada win? / YES"
    assert response.body["wallets"][0]["user_address"] == "0xabc"
    assert response.body["positions"] == []
    assert "slug = 'fifwc-can-bih-2026-06-12'" in fake.queries[0]
    assert "upper(ifNull(tokens.outcome, '')) as outcome_side" in fake.queries[2]
    assert "selection" in fake.queries[3]


def test_product_api_alerts_and_liquidity_routes() -> None:
    fake = FakeClickHouse('{"token_id":"t1"}\n')
    api = ProductApi(clickhouse=fake)

    alerts = api.handle("/alerts", {"type": ["price_move"]})
    liquidity = api.handle("/markets/liquidity", {"token_id": ["t1"]})

    assert alerts.status == 200
    assert liquidity.status == 200
    assert "alerts" in alerts.body
    assert "liquidity" in liquidity.body


def test_product_api_stats_overview_returns_first_row() -> None:
    fake = FakeClickHouse('{"events":10,"markets":20}\n')
    api = ProductApi(clickhouse=fake)

    response = api.handle("/stats/overview", {})

    assert response.status == 200
    assert response.body == {"overview": {"events": 10, "markets": 20}}
    assert "system.parts" in fake.queries[0]
    assert " final" not in fake.queries[0].lower()


def test_product_api_system_stats_route_does_not_query_clickhouse() -> None:
    fake = FakeClickHouse("")
    api = ProductApi(clickhouse=fake)

    response = api.handle("/stats/system", {})

    assert response.status == 200
    assert response.body["system"]["cpu"]["count"] >= 1
    assert response.body["system"]["memory"]["total_bytes"] >= 0
    assert response.body["system"]["disk"]["total_bytes"] > 0
    assert fake.queries == []


def test_collect_system_stats_has_dashboard_fields() -> None:
    stats = collect_system_stats()

    assert {"collected_at", "cpu", "memory", "disk", "uptime_seconds"} <= stats.keys()
    assert "percent" in stats["cpu"]
    assert "percent" in stats["memory"]
    assert "percent" in stats["disk"]


def test_product_api_market_detail_includes_tokens() -> None:
    fake = FakeClickHouse(
        outputs=[
            '{"market_id":"m1","condition_id":"c1","question":"Q?"}\n',
            '{"token_id":"t1","market_id":"m1","condition_id":"c1","outcome":"Yes","outcome_index":0}\n',
        ],
    )
    api = ProductApi(clickhouse=fake)

    response = api.handle("/markets/detail", {"market_id": ["m1"]})

    assert response.status == 200
    assert response.body["market"]["market_id"] == "m1"
    assert response.body["market"]["tokens"] == [
        {"token_id": "t1", "market_id": "m1", "condition_id": "c1", "outcome": "Yes", "outcome_index": 0}
    ]
    assert "from dim_outcome_token final" in fake.queries[1]


def test_product_api_market_trades_requires_market_or_condition() -> None:
    fake = FakeClickHouse("")
    api = ProductApi(clickhouse=fake)

    response = api.handle("/markets/trades", {"limit": ["5"]})

    assert response.status == 200
    assert "and 1 = 0" in fake.queries[0]
    assert response.body == {"trades": []}


def test_product_api_analytics_routes_are_read_only() -> None:
    fake = FakeClickHouse('{"x":1}\n')
    api = ProductApi(clickhouse=fake)

    routes = [
        ("/markets/overview", {}),
        ("/markets/trending", {"limit": ["3"], "status": ["active"]}),
        ("/categories/summary", {"limit": ["3"]}),
        ("/signals/anomalies", {"severity": ["high"], "limit": ["3"]}),
        ("/wallets/smart-money/activity", {"limit": ["3"]}),
    ]
    for path, query in routes:
        response = api.handle(path, query)
        assert response.status == 200

    joined = "\n".join(fake.queries).lower()
    assert not re.search(r"\b(delete|drop|insert|update)\b", joined)


def test_event_and_wallet_analytics_require_scope() -> None:
    fake = FakeClickHouse("")
    api = ProductApi(clickhouse=fake)

    flow = api.handle("/events/wallet-flow", {})
    pnl = api.handle("/events/pnl-leaderboard", {})
    positions = api.handle("/wallets/live-positions", {"limit": ["5"]})

    assert flow.status == 200
    assert flow.body == {"wallets": []}
    assert pnl.status == 200
    assert pnl.body == {"wallets": []}
    assert positions.status == 200
    assert "and 1 = 0" in fake.queries[0]
