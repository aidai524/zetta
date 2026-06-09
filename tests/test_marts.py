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
