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
