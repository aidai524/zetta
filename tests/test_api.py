import json
import re
from types import SimpleNamespace

from zetta.api import (
    ProductApi,
    ch_string,
    collect_system_stats,
    int_param,
    rows_json,
    unusual_betting_summary_response,
    unusual_betting_cache_key,
)
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


def test_unusual_betting_cache_key_ignores_display_limits() -> None:
    first = unusual_betting_cache_key(
        event_ids=["2", "1"],
        cold_price_threshold=0.25,
        large_threshold=500_000,
        very_large_threshold=1_000_000,
        extreme_threshold=5_000_000,
        include_related_markets=True,
    )
    second = unusual_betting_cache_key(
        event_ids=["1", "2"],
        cold_price_threshold=0.25,
        large_threshold=500_000,
        very_large_threshold=1_000_000,
        extreme_threshold=5_000_000,
        include_related_markets=True,
    )

    assert first == second


def test_unusual_betting_summary_uses_persisted_cache() -> None:
    class FakeCacheStore:
        def get(self, _cache_key, *, max_age_seconds=None):
            return {
                "cache_key": "cache-1",
                "refreshed_at": "2026-06-17T00:00:00+00:00",
                "generated_at": "2026-06-17T00:00:00+00:00",
                "age_seconds": 12.0,
                "trigger_reason": "scheduled",
                "error": None,
                "detail": {
                    "status": "ok",
                    "event": {
                        "event_id": "event-1",
                        "slug": "fifwc-esp-cvi-2026-06-15",
                        "title": "Spain vs. Cabo Verde",
                    },
                    "parameters": {
                        "wallet_limit": 100,
                        "trade_limit": 50,
                        "large_threshold": 500000.0,
                        "cold_price_threshold": 0.25,
                    },
                    "analysis": {
                        "severity": "medium",
                        "large_signal_wallet_count": 1,
                        "very_large_signal_wallet_count": 0,
                        "extreme_signal_wallet_count": 0,
                        "signal_total_notional": 600000.0,
                        "signal_outcome_count": 1,
                        "thresholds": {"large_threshold": 500000.0},
                    },
                    "signal_wallet_summary": {
                        "signal_wallet_count": 1,
                        "abnormal_wallet_count": 1,
                        "max_abnormal_wallet_notional": 600000.0,
                    },
                    "signal_wallets": [
                        {
                            "user_address": "0xabc",
                            "total_notional": 600000.0,
                            "max_notional": 300000.0,
                            "fills": 2,
                        }
                    ],
                    "signal_trades": [],
                    "signal_outcomes": [],
                    "generated_at": "2026-06-17T00:00:00+00:00",
                },
            }

    event_row = (
        '{"event_id":"event-1","slug":"fifwc-esp-cvi-2026-06-15",'
        '"title":"Spain vs. Cabo Verde"}\n'
    )
    fake = FakeClickHouse(outputs=[event_row, event_row])
    api = ProductApi(clickhouse=fake, settings=Settings(postgres_dsn="postgresql://example"))
    api._unusual_betting_cache_store = FakeCacheStore()

    response = api.handle(
        "/events/unusual-betting/summary",
        {"slug": ["fifwc-esp-cvi-2026-06-15"]},
    )

    assert response.status == 200
    assert response.body["cache"]["source"] == "postgres_cache"
    assert response.body["abnormal_wallet_count"] == 1
    assert response.body["max_abnormal_wallet_notional"] == 600000.0
    assert "from fact_exchange_fill" not in "\n".join(fake.queries)


def test_unusual_betting_summary_filters_after_wallet_aggregation() -> None:
    detail = {
        "event": {"event_id": "event-1", "slug": "fifwc-test", "title": "Test Match"},
        "parameters": {"large_threshold": 500000.0, "cold_price_threshold": 0.25},
        "analysis": {
            "severity": "medium",
            "large_signal_wallet_count": 1,
            "very_large_signal_wallet_count": 0,
            "extreme_signal_wallet_count": 0,
            "signal_total_notional": 600000.0,
            "signal_outcome_count": 2,
            "thresholds": {"large_threshold": 500000.0},
        },
        "signal_wallet_summary": {
            "signal_wallet_count": 1,
            "abnormal_wallet_count": 1,
            "max_abnormal_wallet_notional": 600000.0,
        },
        "signal_wallets": [
            {
                "user_address": "0xabc",
                "market_slug": "m1",
                "question": "Q1",
                "outcome": "Senegal",
                "total_notional": 300000.0,
                "max_notional": 300000.0,
                "fills": 1,
            },
            {
                "user_address": "0xabc",
                "market_slug": "m2",
                "question": "Q2",
                "outcome": "No",
                "total_notional": 300000.0,
                "max_notional": 300000.0,
                "fills": 1,
            },
        ],
        "signal_trades": [],
        "signal_outcomes": [],
    }

    summary = unusual_betting_summary_response(detail)

    assert summary["abnormal_wallet_count"] == 1
    assert summary["abnormal_wallets"][0]["user_address"] == "0xabc"
    assert summary["abnormal_wallets"][0]["total_notional"] == 600000.0


def test_product_api_market_search_worldcup_scope_adds_filter() -> None:
    fake = FakeClickHouse('{"market_id":"m1","question":"Will Canada win?"}\n')
    api = ProductApi(clickhouse=fake)

    response = api.handle(
        "/markets/search",
        {"scope": ["world_cup"], "q": ["World Cup"], "limit": ["1"]},
    )

    assert response.status == 200
    assert response.body["markets"][0]["market_id"] == "m1"
    query = fake.queries[0]
    assert "startsWith(events.slug, 'fifwc-')" in query
    assert "World Cup') > 0" in query


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


def test_product_api_wallet_screener_filters_by_category_and_range() -> None:
    fake = FakeClickHouse("")
    api = ProductApi(clickhouse=fake)

    response = api.handle(
        "/wallets/screener",
        {"mode": ["smart"], "category": ["体育"], "range": ["7d"], "limit": ["5"]},
    )

    assert response.status == 200
    query = fake.queries[0]
    assert "from fact_trade_by_user as trades" in query
    assert "trades.timestamp >= now64(3) - interval 7 day" in query
    assert "positionCaseInsensitive" in query
    assert "'Sports'" in query
    assert "category_traded_notional" in query


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
    assert body["wallet"]["data_freshness_status"] == "ok"
    assert body["wallet"]["pnl_lag_minutes"] == 0.0
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


def test_product_api_wallet_detail_live_uses_polymarket_wallet_apis(monkeypatch) -> None:
    def page(items):
        return SimpleNamespace(response=SimpleNamespace(body=items, url="https://example.test"), items=items)

    class FakePolymarketClient:
        def __init__(self, settings) -> None:
            self.settings = settings

        def data_positions(self, *, user):
            return page(
                [
                    {
                        "proxyWallet": user,
                        "asset": "asset-1",
                        "conditionId": "c1",
                        "title": "Will Belgium win?",
                        "slug": "fifwc-bel-egy-win",
                        "eventSlug": "fifwc-bel-egy",
                        "outcome": "Yes",
                        "size": 100,
                        "avgPrice": 0.5,
                        "curPrice": 0.7,
                        "initialValue": 50,
                        "currentValue": 70,
                        "cashPnl": 20,
                    }
                ]
            )

        def data_value(self, *, user):
            return page([{"user": user, "value": 70}])

        def user_pnl(self, *, user, interval, fidelity):
            return page([{"t": 1781481600, "p": -636.8}, {"t": 1781568000, "p": -500.0}])

        def data_activity(self, *, user, limit, offset):
            return page(
                [
                    {
                        "proxyWallet": user,
                        "timestamp": "2026-06-15T19:39:43Z",
                        "type": "TRADE",
                        "conditionId": "c1",
                        "asset": "asset-1",
                        "transactionHash": "0xhash2",
                        "side": "SELL",
                        "price": 0.8,
                        "size": 10,
                        "usdcSize": 8,
                        "title": "Newest trade",
                        "slug": "newest-trade",
                        "eventSlug": "event-newest",
                        "outcome": "Yes",
                    },
                    {
                        "proxyWallet": user,
                        "timestamp": "2026-06-15T18:39:43Z",
                        "type": "TRADE",
                        "conditionId": "c1",
                        "asset": "asset-1",
                        "transactionHash": "0xhash1",
                        "side": "BUY",
                        "price": 0.6,
                        "size": 25,
                        "usdcSize": 15,
                        "title": "Older trade",
                        "slug": "older-trade",
                        "eventSlug": "event-older",
                        "outcome": "Yes",
                    },
                ]
            )

    monkeypatch.setattr("zetta.api.PolymarketClient", FakePolymarketClient)
    monkeypatch.setattr(ProductApi, "live_pusd_balance", lambda _self, _user: 12.5)
    fake = FakeClickHouse(outputs=[""])
    api = ProductApi(clickhouse=fake, settings=Settings())

    response = api.handle("/wallets/detail", {"user": ["0xABC"], "live": ["1"]})

    assert response.status == 200
    body = response.body
    assert body["wallet"]["data_source"] == "live"
    assert body["wallet"]["cash"] == 12.5
    assert body["wallet"]["portfolio_value"] == 82.5
    assert body["wallet"]["latest_total_pnl"] == -500.0
    assert body["positions"][0]["title"] == "Will Belgium win?"
    assert body["activity_summary"]["trade_activity_count"] == 2
    assert body["activity_summary"]["traded_notional"] == 23.0
    assert body["recent_activity"][0]["title"] == "Newest trade"
    assert "mart_wallet_reputation" in fake.queries[0]


def test_product_api_live_trades_uses_polymarket_data_api(monkeypatch) -> None:
    def page(items):
        return SimpleNamespace(response=SimpleNamespace(body=items, url="https://example.test/trades"), items=items)

    class FakePolymarketClient:
        def __init__(self, settings) -> None:
            self.settings = settings
            self.calls = []

        def data_trades(self, *, limit, offset):
            self.calls.append((limit, offset))
            assert limit == 100
            return page(
                [
                    {
                        "proxyWallet": "0xABC",
                        "side": "BUY",
                        "asset": "token-1",
                        "conditionId": "cond-1",
                        "size": 10,
                        "price": 0.42,
                        "timestamp": 1781599722,
                        "title": "Will Bitcoin rise?",
                        "slug": "bitcoin-rise",
                        "eventSlug": "bitcoin",
                        "outcome": "Yes",
                        "name": "trader",
                        "pseudonym": "Alias",
                        "transactionHash": "0xhash",
                    },
                    {
                        "proxyWallet": "0xABC",
                        "side": "BUY",
                        "asset": "token-1",
                        "conditionId": "cond-1",
                        "size": 10,
                        "price": 0.42,
                        "timestamp": 1781599722,
                        "title": "Will Bitcoin rise?",
                        "slug": "bitcoin-rise",
                        "eventSlug": "bitcoin",
                        "outcome": "Yes",
                        "name": "trader",
                        "pseudonym": "Alias",
                        "transactionHash": "0xhash",
                    },
                    {
                        "proxyWallet": "0xDEF",
                        "side": "SELL",
                        "asset": "token-2",
                        "conditionId": "cond-2",
                        "size": 5,
                        "price": 0.8,
                        "timestamp": 1781599730 + offset,
                        "title": "Will Ethereum rise?",
                        "slug": "ethereum-rise",
                        "eventSlug": "ethereum",
                        "outcome": "No",
                        "name": "seller",
                        "pseudonym": "Seller",
                        "transactionHash": f"0xhash{offset}",
                    },
                ]
            )

    monkeypatch.setattr("zetta.api.PolymarketClient", FakePolymarketClient)
    fake = FakeClickHouse("")
    api = ProductApi(clickhouse=fake, settings=Settings())

    response = api.handle(
        "/trades/live",
        {"limit": ["10"], "ttl": ["0.5"], "pages": ["2"], "min_notional": ["4"], "side": ["BUY"]},
    )

    assert response.status == 200
    body = response.body
    assert body["source"] == "live"
    assert body["status"] == "ok"
    assert body["request_url"] == "https://example.test/trades"
    assert body["request_urls"] == ["https://example.test/trades", "https://example.test/trades"]
    assert body["candidate_count"] == 4
    assert fake.queries == []
    assert len(body["trades"]) == 1
    trade = body["trades"][0]
    assert trade["timestamp"] == "2026-06-16 08:48:42.000"
    assert trade["question"] == "Will Bitcoin rise?"
    assert trade["outcome"] == "Yes"
    assert trade["user_address"] == "0xabc"
    assert trade["notional"] == 4.2
    assert trade["source"] == "polymarket-live"
    assert len(trade["trade_id"]) == 40
    assert isinstance(body["latency_seconds"], float)


def test_product_api_live_trades_prefers_chain_fills(monkeypatch) -> None:
    class UnexpectedPolymarketClient:
        def __init__(self, settings) -> None:
            self.settings = settings

        def data_trades(self, **_kwargs):
            raise AssertionError("data api should not be called when chain rows are available")

    row = {
        "trade_id": "0xhash-1-maker-token-1",
        "transaction_hash": "0xhash",
        "log_index": 1,
        "timestamp": "2026-06-16 08:48:42.000",
        "market_id": "market-1",
        "condition_id": "cond-1",
        "token_id": "token-1",
        "user_address": "0xabc",
        "side": "BUY",
        "price": 0.42,
        "size": 10,
        "notional": 4.2,
        "source": "chain-live",
        "ingested_at": "2026-06-16 08:48:42.000",
        "question": "Will Bitcoin rise?",
        "market_slug": "bitcoin-rise",
        "event_id": "event-1",
        "event_title": "Bitcoin",
        "event_slug": "bitcoin",
        "category": "Crypto",
        "outcome": "Yes",
        "trader_name": "",
        "trader_pseudonym": "",
        "is_smart": False,
        "is_whale": False,
        "wallet_total_pnl": 0,
        "wallet_pnl_roi": 0,
        "wallet_traded_notional": 0,
    }
    fake = FakeClickHouse(json.dumps(row) + "\n")
    monkeypatch.setattr("zetta.api.PolymarketClient", UnexpectedPolymarketClient)
    api = ProductApi(clickhouse=fake, settings=Settings())

    response = api.handle("/trades/live", {"limit": ["5"], "ttl": ["0.5"]})

    assert response.status == 200
    body = response.body
    assert body["source"] == "chain-live"
    assert body["request_url"] == "clickhouse:fact_exchange_fill"
    assert body["metadata_missing_count"] == 0
    assert body["trades"][0]["source"] == "chain-live"
    assert body["trades"][0]["question"] == "Will Bitcoin rise?"
    assert "from fact_exchange_fill" in fake.queries[0]


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


def test_product_api_tracked_wallets_get_post_delete(monkeypatch) -> None:
    captured = {"refresh": []}

    class FakeTrackedWalletStore:
        def __init__(self, **kwargs) -> None:
            captured["store_kwargs"] = kwargs

        def list_wallets(self):
            return [{"address": "0xabc0000000000000000000000000000000000000", "name": "Alpha"}]

        def upsert_wallet(self, *, user_address, name):
            captured["upsert"] = (user_address, name)
            return {"address": user_address, "user_address": user_address, "name": name}

        def delete_wallet(self, *, user_address):
            captured["delete"] = user_address
            return True

    class FakeTaskStore:
        def __init__(self, **_kwargs) -> None:
            pass

        def add_many(self, tasks):
            captured["refresh"].extend(tasks)
            return len(tasks)

    monkeypatch.setattr("zetta.api.TrackedWalletStore", FakeTrackedWalletStore)
    monkeypatch.setattr("zetta.api.PostgresTaskStore", FakeTaskStore)
    api = ProductApi(clickhouse=FakeClickHouse(), settings=Settings(postgres_dsn="postgresql://example"))

    listed = api.handle_request("GET", "/wallets/tracked", {}, None)
    posted = api.handle_request(
        "POST",
        "/wallets/tracked",
        {},
        {"address": "0xABC0000000000000000000000000000000000000", "name": "Alpha"},
    )
    deleted = api.handle_request(
        "DELETE",
        "/wallets/tracked",
        {},
        {"address": "0xABC0000000000000000000000000000000000000"},
    )

    assert listed.status == 200
    assert listed.body["wallets"][0]["name"] == "Alpha"
    assert posted.status == 200
    assert posted.body["wallet"]["address"] == "0xabc0000000000000000000000000000000000000"
    assert deleted.status == 200
    assert deleted.body["deleted"] is True
    assert captured["store_kwargs"]["dsn"] == "postgresql://example"
    assert captured["upsert"] == ("0xabc0000000000000000000000000000000000000", "Alpha")
    assert captured["delete"] == "0xabc0000000000000000000000000000000000000"
    assert {task.kind for task in captured["refresh"]} == {
        "wallet-activity",
        "wallet-pnl",
        "wallet-portfolio",
        "wallet-trades",
    }


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
            '{"market_id":"m1","last_price":0.55,"volume_24h":12.5}\n',
            '{"token_id":"t1","market_id":"m1","condition_id":"c1","outcome":"Yes","outcome_index":0}\n',
        ],
    )
    api = ProductApi(clickhouse=fake)

    response = api.handle("/markets/detail", {"market_id": ["m1"]})

    assert response.status == 200
    assert response.body["market"]["market_id"] == "m1"
    assert response.body["market"]["last_price"] == 0.55
    assert response.body["market"]["volume_24h"] == 12.5
    assert response.body["market"]["tokens"] == [
        {"token_id": "t1", "market_id": "m1", "condition_id": "c1", "outcome": "Yes", "outcome_index": 0}
    ]
    assert "from dim_outcome_token final" in fake.queries[2]


def test_product_api_market_trades_requires_market_or_condition() -> None:
    fake = FakeClickHouse("")
    api = ProductApi(clickhouse=fake)

    response = api.handle("/markets/trades", {"limit": ["5"]})

    assert response.status == 200
    assert "and 1 = 0" in fake.queries[0]
    assert response.body == {"trades": []}


def test_product_api_recent_trades_is_global_and_filterable() -> None:
    fake = FakeClickHouse(
        '{"user_address":"0xabc","side":"BUY","notional":125.5,'
        '"question":"Q?","category":"Sports","outcome":"Yes"}\n'
    )
    api = ProductApi(clickhouse=fake)

    response = api.handle(
        "/trades/recent",
        {
            "limit": ["5"],
            "side": ["buy"],
            "wallet_type": ["smart"],
            "category": ["Sports"],
            "min_notional": ["100"],
            "q": ["abc"],
        },
    )

    assert response.status == 200
    assert response.body["trades"][0]["notional"] == 125.5
    query = fake.queries[0]
    assert "from fact_trade_by_time as trades" in query
    assert "trades.side = 'BUY'" in query
    assert "trades.notional >= 100.0" in query
    assert "events.category = 'Sports'" in query
    assert "ifNull(screener.is_smart, false)" in query
    assert "positionCaseInsensitive(user_address, 'abc')" in query


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
