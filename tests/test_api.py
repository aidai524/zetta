from zetta.api import ProductApi, ch_string, collect_system_stats, int_param, rows_json


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
    assert "delete" not in joined
    assert "drop" not in joined
    assert "insert" not in joined
    assert "update" not in joined


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
