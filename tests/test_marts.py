from zetta.loaders.marts import MartBuilder


class FakeClickHouse:
    def __init__(self) -> None:
        self.executed = []
        self.tables = {}

    def execute(self, query):
        self.executed.append(query)
        return ""

    def query_text(self, query):
        if "mart_trader_profile" in query or "mart_trader_chain_pnl" in query:
            return "0"
        return "0"

    def insert(self, table, rows):
        self.tables.setdefault(table, []).extend(rows)
        return len(rows)


def test_build_trader_profiles_includes_chain_pnl_fields() -> None:
    fake = FakeClickHouse()

    result = MartBuilder(clickhouse=fake).build_trader_profiles()

    assert result.mart == "trader_profile"
    joined_sql = "\n".join(fake.executed)
    assert "insert into mart_trader_chain_pnl" in joined_sql
    assert "chain_mark_to_market_pnl" in joined_sql
    assert "mart_trader_chain_pnl final" in joined_sql


def test_build_event_wallet_pnl_uses_resolution_prices() -> None:
    fake = FakeClickHouse()

    result = MartBuilder(clickhouse=fake).build_event_wallet_pnl()

    assert result.mart == "event_wallet_pnl"
    joined_sql = "\n".join(fake.executed)
    assert "insert into mart_event_wallet_pnl" in joined_sql
    assert "outcomePrices" in joined_sql
    assert "final_position_value" in joined_sql
    assert "data_api_estimate" in joined_sql


def test_build_live_wallet_positions_marks_active_positions() -> None:
    fake = FakeClickHouse()

    result = MartBuilder(clickhouse=fake).build_live_wallet_positions()

    assert result.mart == "live_wallet_position"
    joined_sql = "\n".join(fake.executed)
    assert "insert into mart_live_wallet_position" in joined_sql
    assert "orderbook_mid" in joined_sql
    assert "price_history" in joined_sql
    assert "unrealized_pnl_estimate" in joined_sql


def test_build_wallet_reputation_depends_on_event_and_live_marts() -> None:
    fake = FakeClickHouse()

    result = MartBuilder(clickhouse=fake).build_wallet_reputation()

    assert result.mart == "wallet_reputation"
    joined_sql = "\n".join(fake.executed)
    assert "insert into mart_wallet_reputation" in joined_sql
    assert "mart_event_wallet_pnl" in joined_sql
    assert "mart_live_wallet_position" in joined_sql


def test_build_wallet_trade_rollup_uses_time_indexed_trades() -> None:
    fake = FakeClickHouse()

    result = MartBuilder(clickhouse=fake).build_wallet_trade_rollup(since_hours=24)

    assert result.mart == "wallet_trade_rollup"
    joined_sql = "\n".join(fake.executed)
    assert "insert into mart_wallet_trade_rollup" in joined_sql
    assert "from fact_trade_by_time" in joined_sql


def test_build_wallet_screener_uses_full_trade_history_and_portfolio_snapshot() -> None:
    fake = FakeClickHouse()

    result = MartBuilder(clickhouse=fake).build_wallet_screener()

    assert result.mart == "wallet_screener"
    joined_sql = "\n".join(fake.executed)
    assert "insert into mart_wallet_screener" in joined_sql
    assert "from fact_trade" in joined_sql
    assert "fact_wallet_portfolio_snapshot" in joined_sql
    assert "max_single_trade_notional" in joined_sql
    assert "pnl_roi >= 0.55" in joined_sql


def test_build_wallet_fifa_24h_pnl_uses_equity_delta_scope() -> None:
    fake = FakeClickHouse()

    result = MartBuilder(clickhouse=fake).build_wallet_fifa_24h_pnl(window_hours=24)

    assert result.mart == "wallet_fifa_24h_pnl"
    joined_sql = "\n".join(fake.executed)
    assert "insert into mart_fifa_trade" in joined_sql
    assert "truncate table mart_wallet_fifa_24h_pnl_next" in joined_sql
    assert "insert into mart_wallet_fifa_24h_pnl_next" in joined_sql
    assert "startsWith(markets.slug, 'fifwc-')" in joined_sql
    assert "equity_now - equity_24h_ago as pnl_24h" in joined_sql
    assert "position_size_24h_ago" in joined_sql
    assert "from mart_fifa_trade as trades final" in joined_sql
    assert "exchange tables mart_wallet_fifa_24h_pnl and mart_wallet_fifa_24h_pnl_next" in joined_sql


def test_build_fifa_trades_uses_time_indexed_fact_trade_cache() -> None:
    fake = FakeClickHouse()

    result = MartBuilder(clickhouse=fake).build_fifa_trades(window_hours=72)

    assert result.mart == "fifa_trade"
    joined_sql = "\n".join(fake.executed)
    assert "insert into mart_fifa_trade" in joined_sql
    assert "from fact_trade_by_time" in joined_sql
    assert "startsWith(markets.slug, 'fifwc-')" in joined_sql
    assert "existing_max" in joined_sql
    assert "round(price, 12)" in joined_sql
    assert "round(size, 6)" in joined_sql
    assert "round(notional, 6)" in joined_sql


def test_build_event_anomaly_signals_are_evidence_signals() -> None:
    fake = FakeClickHouse()

    result = MartBuilder(clickhouse=fake).build_event_anomaly_signals(
        large_trade_threshold=123.0,
        liquidity_ratio_threshold=0.2,
        coordinated_wallet_threshold=3,
        coordinated_notional_threshold=456.0,
        since_hours=12,
    )

    assert result.mart == "event_anomaly_signal"
    joined_sql = "\n".join(fake.executed)
    assert "insert into mart_event_anomaly_signal" in joined_sql
    assert "123.0" in joined_sql
    assert "coordinated-like signal only" in joined_sql
    assert "uncertainty" in joined_sql


def test_build_analytics_core_builds_marts_in_dependency_order() -> None:
    fake = FakeClickHouse()

    results = MartBuilder(clickhouse=fake).build_analytics_core()

    assert [result.mart for result in results] == [
        "event_wallet_pnl",
        "live_wallet_position",
        "wallet_reputation",
        "event_anomaly_signal",
    ]
