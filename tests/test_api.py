from zetta.api import ProductApi, ch_string, int_param, rows_json


class FakeClickHouse:
    def __init__(self, output="") -> None:
        self.output = output
        self.queries = []

    def query_text(self, query):
        self.queries.append(query)
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
