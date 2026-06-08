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
    api = ProductApi(clickhouse=FakeClickHouse(""))

    response = api.handle("/traders/profile", {"user": ["0xabc"]})

    assert response.status == 404
    assert response.body == {"error": "trader_not_found"}


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
