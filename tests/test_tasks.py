from datetime import UTC, datetime
import json

from zetta.config import Settings
from zetta.cli import (
    ACTIVE_EVENT_WALLET_PRIORITY,
    DISCOVERY_PRIORITY,
    FRONTIER_EVENT_PRIORITY,
    FRONTIER_BOOK_PRIORITY,
    FRONTIER_GAMMA_PRIORITY,
    FRONTIER_PRICE_HISTORY_PRIORITY,
    FRONTIER_TRADES_PRIORITY,
    WALLET_EXPLICIT_REFRESH_PRIORITY,
    WALLET_REFRESH_PRIORITY,
    UNUSUAL_BETTING_REFRESH_PRIORITY,
    cmd_tasks_seed_basic,
    cmd_tasks_seed_active_event_wallets,
    cmd_tasks_seed_frontier,
    cmd_tasks_seed_history,
    cmd_tasks_seed_unusual_betting,
    cmd_tasks_seed_wallets,
    parse_task_kinds,
)
from zetta.scheduler.runner import TaskRunner, task_execution_params
from zetta.scheduler.tasks import (
    LocalRunStore,
    LocalTaskStore,
    PostgresTaskStore,
    Task,
    requeue_done_task,
    row_to_task,
    task_dedupe_value,
    task_source_entity,
)


def test_local_task_store_deduplicates_and_claims(tmp_path) -> None:
    store = LocalTaskStore(tmp_path / "tasks.json")
    added = store.add_many(
        [
            Task(kind="gamma-events", params={"page_limit": 100}),
            Task(kind="gamma-events", params={"page_limit": 100}),
        ]
    )

    assert added == 1
    assert store.summary()["pending"] == 1

    task = store.claim_next()

    assert task is not None
    assert task.kind == "gamma-events"
    assert store.summary()["running"] == 1

    store.complete(task.id)

    assert store.summary()["done"] == 1


def test_local_task_store_claims_lowest_priority_first(tmp_path) -> None:
    store = LocalTaskStore(tmp_path / "tasks.json")
    store.add_many(
        [
            Task(kind="trades", params={"market": "condition-1"}, priority=100),
            Task(kind="gamma-events", params={"page_limit": 100}, priority=10),
            Task(kind="book", params={"token_id": "token-1"}, priority=40),
        ]
    )

    task = store.claim_next()

    assert task is not None
    assert task.kind == "gamma-events"


def test_unusual_betting_task_source_entity() -> None:
    assert task_source_entity("unusual-betting-refresh") == (
        "event",
        "unusual_betting_refresh",
    )


def test_wallet_fifa_24h_pnl_task_source_entity() -> None:
    assert task_source_entity("wallet-fifa-24h-pnl") == (
        "mart",
        "wallet_fifa_24h_pnl",
    )


def test_local_task_store_filters_allowed_kinds(tmp_path) -> None:
    seed_store = LocalTaskStore(tmp_path / "tasks.json")
    seed_store.add_many(
        [
            Task(kind="trades", params={"market": "condition-1"}, priority=0),
            Task(kind="wallet-pnl", params={"user": "0x1"}, priority=10),
            Task(kind="wallet-portfolio", params={"user": "0x2"}, priority=20),
        ]
    )
    wallet_store = LocalTaskStore(
        tmp_path / "tasks.json",
        allowed_kinds={"wallet-portfolio", "wallet-pnl"},
    )

    first = wallet_store.claim_next()
    second = wallet_store.claim_next()
    third = wallet_store.claim_next()

    assert first is not None
    assert first.kind == "wallet-pnl"
    assert second is not None
    assert second.kind == "wallet-portfolio"
    assert third is None
    tasks = seed_store.load()
    assert [task.kind for task in tasks if task.status == "pending"] == ["trades"]


def test_postgres_task_store_claim_orders_kind_filter_before_lease_params() -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self.sql = ""
            self.params = None

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def execute(self, sql, params) -> None:
            self.sql = sql
            self.params = params

        def fetchone(self):
            return None

    now = datetime(2026, 1, 1, tzinfo=UTC)
    cursor = FakeCursor()
    store = PostgresTaskStore(
        dsn="postgresql://example",
        node_id="wallet-helper-2-1",
        allowed_kinds={"wallet-pnl", "wallet-portfolio"},
    )

    store._claim_with_status(cursor, where_sql="status = 'pending'", lease_expires_at=now)

    assert "task_type = any(%s::text[])" in cursor.sql
    assert cursor.params == [
        ["wallet-pnl", "wallet-portfolio"],
        "wallet-helper-2-1",
        now,
    ]


def test_postgres_task_store_node_progress_uses_lookback_parameter(monkeypatch) -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self.sql = ""
            self.params = None

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def execute(self, sql, params) -> None:
            self.sql = sql
            self.params = params

        def fetchall(self):
            return [
                (
                    "wallet-helper-1-1",
                    {"wallet-pnl": {"runs": 2, "done": 2, "items": 10, "pages": 2, "avg_seconds": 0.5}},
                    {"wallet-pnl": {"running_tasks": 1}},
                    2,
                    2,
                    0,
                    10,
                    2,
                    1,
                    now,
                )
            ]

    class FakeConnection:
        def __init__(self, cursor) -> None:
            self.cursor_instance = cursor

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def cursor(self):
            return self.cursor_instance

    class FakePsycopg:
        def __init__(self, cursor) -> None:
            self.cursor_instance = cursor

        def connect(self, _dsn):
            return FakeConnection(self.cursor_instance)

    now = datetime(2026, 1, 1, tzinfo=UTC)
    cursor = FakeCursor()
    monkeypatch.setattr("zetta.scheduler.tasks.import_psycopg", lambda: (FakePsycopg(cursor), object))
    store = PostgresTaskStore(dsn="postgresql://example", node_id="api")

    result = store.node_progress(lookback_minutes=15)

    assert cursor.params == (15,)
    assert "collector_runs" in cursor.sql
    assert result["lookback_minutes"] == 15
    assert result["totals"]["nodes"] == 1
    assert result["totals"]["running_tasks"] == 1
    assert result["nodes"][0]["role"] == "wallet-helper"


def test_postgres_task_store_progress_uses_database_aggregates(monkeypatch) -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self.executed = []
            self.index = 0

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def execute(self, sql, params=None) -> None:
            self.executed.append((sql, params))

        def fetchall(self):
            self.index += 1
            if self.index == 1:
                return [
                    ("wallet-pnl", "done", 5),
                    ("wallet-pnl", "running", 1),
                    ("trades", "pending", 2),
                ]
            if self.index == 2:
                return [("wallet-pnl", "running", 1, now)]
            return [
                (
                    123,
                    "wallet-pnl",
                    "wallet-helper-1-1",
                    now,
                    now,
                    "done",
                    1,
                    10,
                    0.1,
                    None,
                )
            ]

    class FakeConnection:
        def __init__(self, cursor) -> None:
            self.cursor_instance = cursor

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def cursor(self):
            return self.cursor_instance

    class FakePsycopg:
        def __init__(self, cursor) -> None:
            self.cursor_instance = cursor

        def connect(self, _dsn):
            return FakeConnection(self.cursor_instance)

    now = datetime(2026, 1, 1, tzinfo=UTC)
    cursor = FakeCursor()
    monkeypatch.setattr("zetta.scheduler.tasks.import_psycopg", lambda: (FakePsycopg(cursor), object))
    store = PostgresTaskStore(dsn="postgresql://example", node_id="api")

    result = store.progress(recent_limit=8)

    assert "group by task_type, status" in cursor.executed[0][0]
    assert "from collector_tasks\n                    where status" in cursor.executed[1][0]
    assert cursor.executed[1][1] == (8,)
    assert result["summary"] == {
        "pending": 2,
        "running": 1,
        "done": 5,
        "failed": 0,
        "dead_lettered": 0,
    }
    assert result["by_kind"]["wallet-pnl"]["done_percent"] == 83.33
    assert result["active"][0]["kind"] == "wallet-pnl"


def test_parse_task_kinds_accepts_repeated_and_comma_values() -> None:
    assert parse_task_kinds(["wallet-portfolio,wallet-pnl", "activity"]) == {
        "activity",
        "wallet-pnl",
        "wallet-portfolio",
    }
    assert parse_task_kinds([]) is None


def test_task_source_entity_maps_known_task_kinds() -> None:
    assert task_source_entity("event-refresh") == ("event", "refresh")
    assert task_source_entity("gamma-events") == ("gamma", "events")
    assert task_source_entity("prices-history") == ("clob", "prices_history")
    assert task_source_entity("activity") == ("data", "activity")
    assert task_source_entity("wallet-activity") == ("data", "activity")
    assert task_source_entity("wallet-trades") == ("data", "trades")
    assert task_source_entity("market-positions") == ("data", "market_positions")
    assert task_source_entity("positions") == ("data", "positions")
    assert task_source_entity("wallet-portfolio") == ("data", "wallet_portfolio")
    assert task_source_entity("chain-logs") == ("polygon", "logs")


def test_local_task_store_reports_progress(tmp_path) -> None:
    store = LocalTaskStore(tmp_path / "tasks.json")
    store.add_many(
        [
            Task(kind="gamma-events", params={"page_limit": 100}),
            Task(kind="trades", params={"market": "condition-1"}),
            Task(kind="trades", params={"market": "condition-2"}),
        ]
    )
    claimed = store.claim_next()
    assert claimed is not None
    store.complete(claimed.id)

    progress = store.progress()

    assert progress["total_tasks"] == 3
    assert progress["summary"] == {
        "dead_lettered": 0,
        "done": 1,
        "failed": 0,
        "pending": 2,
        "running": 0,
    }
    assert progress["done_percent"] == 33.33
    assert progress["by_kind"]["gamma-events"]["done"] == 1
    assert progress["by_kind"]["trades"]["pending"] == 2
    assert len(progress["active"]) == 2


def test_row_to_task_normalizes_postgres_row() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    task = row_to_task(
        (123, "gamma-events", {"page_limit": 100}, "running", 2, now, now, None, 7, 10)
    )

    assert task.id == "123"
    assert task.kind == "gamma-events"
    assert task.params == {"page_limit": 100}
    assert task.status == "running"
    assert task.attempts == 2
    assert task.max_attempts == 7
    assert task.priority == 10


def test_task_execution_params_drop_scheduler_metadata() -> None:
    assert task_execution_params(
        {
            "page_limit": 100,
            "_refresh_run": "2026-06-08T00:00:00+00:00",
            "_dedupe_key": "wallet-refresh:abc",
        }
    ) == {"page_limit": 100}


def test_local_task_store_can_dedupe_by_stable_key(tmp_path) -> None:
    store = LocalTaskStore(tmp_path / "tasks.json")
    store.add_many(
        [
            Task(
                kind="unusual-betting-refresh",
                params={
                    "slug": "fifwc-esp-cvi-2026-06-15",
                    "trigger_reason": "scheduled",
                    "_dedupe_key": "unusual-betting|fifwc-esp-cvi-2026-06-15",
                },
            )
        ]
    )
    store.complete(store.load()[0].id)

    added = store.add_many(
        [
            Task(
                kind="unusual-betting-refresh",
                params={
                    "slug": "fifwc-esp-cvi-2026-06-15",
                    "trigger_reason": "active-event-wallets",
                    "_dedupe_key": "unusual-betting|fifwc-esp-cvi-2026-06-15",
                    "_requeue_done": True,
                },
                priority=UNUSUAL_BETTING_REFRESH_PRIORITY,
            )
        ]
    )

    tasks = store.load()
    assert added == 1
    assert len(tasks) == 1
    assert tasks[0].status == "pending"
    assert tasks[0].params["trigger_reason"] == "active-event-wallets"
    assert task_dedupe_value(tasks[0].params) == "unusual-betting|fifwc-esp-cvi-2026-06-15"


def test_seed_basic_keeps_global_trade_sample_finite_when_gamma_is_unbounded(tmp_path) -> None:
    args = type(
        "Args",
        (),
        {
            "task_store": "local",
            "task_file": str(tmp_path / "tasks.json"),
            "node_id": "node-1",
            "lease_seconds": 300,
            "page_limit": 100,
            "max_pages": 0,
        },
    )()

    result = cmd_tasks_seed_basic(args, Settings())
    tasks = LocalTaskStore(tmp_path / "tasks.json").load()
    trade_task = next(task for task in tasks if task.kind == "trades")

    assert result["added"] == 3
    assert trade_task.params["max_pages"] == 1
    assert trade_task.params["resume"] is False
    assert {task.priority for task in tasks} == {DISCOVERY_PRIORITY}


def test_seed_wallets_adds_wallet_trade_and_activity_tasks(monkeypatch, tmp_path) -> None:
    class FakeClickHouse:
        def __init__(self, _settings):
            pass

        def query_text(self, query):
            assert "mart_wallet_trade_rollup" in query
            return '{"user_address":"0xabc"}\n{"user_address":"0xdef"}\n'

    monkeypatch.setattr("zetta.cli.ClickHouseWriter", FakeClickHouse)
    args = type(
        "Args",
        (),
        {
            "task_store": "local",
            "task_file": str(tmp_path / "tasks.json"),
            "node_id": "node-1",
            "lease_seconds": 300,
            "wallet_limit": 10,
            "since_hours": 48,
            "page_limit": 500,
            "max_pages": 2,
            "candidate_mode": "recent",
            "min_notional": 10_000.0,
            "include_trades": True,
            "include_activity": True,
            "include_wallet_portfolio": False,
            "include_wallet_pnl": False,
            "include_tracked_wallets": False,
            "refresh_run": "realtime-wallets",
            "requeue_done": True,
            "wallets": [],
        },
    )()

    result = cmd_tasks_seed_wallets(args, Settings())
    tasks = LocalTaskStore(tmp_path / "tasks.json").load()

    assert result["candidate_tasks"] == 4
    assert result["added"] == 4
    assert [task.kind for task in tasks].count("wallet-trades") == 2
    assert [task.kind for task in tasks].count("wallet-activity") == 2
    assert {task.priority for task in tasks} == {WALLET_REFRESH_PRIORITY}
    assert all(task.params["user"] in {"0xabc", "0xdef"} for task in tasks)
    assert all(requeue_done_task(task.params) for task in tasks)


def test_seed_wallets_can_use_smart_candidate_wallets(monkeypatch, tmp_path) -> None:
    class FakeClickHouse:
        def __init__(self, _settings):
            pass

        def query_text(self, query):
            assert "mart_wallet_screener final" in query
            assert "traded_notional >= 10000.0" in query
            return '{"user_address":"0xabc"}\n{"user_address":"0xdef"}\n'

    monkeypatch.setattr("zetta.cli.ClickHouseWriter", FakeClickHouse)
    args = type(
        "Args",
        (),
        {
            "task_store": "local",
            "task_file": str(tmp_path / "tasks.json"),
            "node_id": "node-1",
            "lease_seconds": 300,
            "wallet_limit": 10,
            "since_hours": 48,
            "page_limit": 500,
            "max_pages": 2,
            "candidate_mode": "smart-candidates",
            "min_notional": 10_000.0,
            "include_trades": False,
            "include_activity": False,
            "include_wallet_portfolio": True,
            "include_wallet_pnl": False,
            "include_tracked_wallets": False,
            "refresh_run": "smart-candidates",
            "requeue_done": True,
            "wallets": [],
        },
    )()

    result = cmd_tasks_seed_wallets(args, Settings())
    tasks = LocalTaskStore(tmp_path / "tasks.json").load()

    assert result["candidate_tasks"] == 2
    assert result["candidate_mode"] == "smart-candidates"
    assert [task.kind for task in tasks] == ["wallet-portfolio", "wallet-portfolio"]
    assert all(task.priority == 0 for task in tasks)
    assert all(requeue_done_task(task.params) for task in tasks)


def test_seed_wallets_allows_explicit_wallet_without_candidate_discovery(monkeypatch, tmp_path) -> None:
    class FakeClickHouse:
        def __init__(self, _settings):
            pass

        def query_text(self, _query):
            raise AssertionError("explicit wallet seeding should not query candidates")

    monkeypatch.setattr("zetta.cli.ClickHouseWriter", FakeClickHouse)
    args = type(
        "Args",
        (),
        {
            "task_store": "local",
            "task_file": str(tmp_path / "tasks.json"),
            "node_id": "node-1",
            "lease_seconds": 300,
            "wallet_limit": 0,
            "since_hours": 48,
            "page_limit": 500,
            "max_pages": 2,
            "candidate_mode": "recent",
            "min_notional": 10_000.0,
            "include_trades": True,
            "include_activity": True,
            "include_wallet_portfolio": True,
            "include_wallet_pnl": True,
            "include_tracked_wallets": False,
            "refresh_run": "manual-wallet",
            "requeue_done": True,
            "wallets": ["0xABC"],
        },
    )()

    result = cmd_tasks_seed_wallets(args, Settings())
    tasks = LocalTaskStore(tmp_path / "tasks.json").load()

    assert result["wallets"] == 1
    assert result["explicit_wallets"] == 1
    assert result["candidate_tasks"] == 4
    assert {task.kind for task in tasks} == {
        "wallet-activity",
        "wallet-trades",
        "wallet-pnl",
        "wallet-portfolio",
    }
    assert {task.params["user"] for task in tasks} == {"0xabc"}
    assert {task.priority for task in tasks} == {WALLET_EXPLICIT_REFRESH_PRIORITY}


def test_seed_wallets_includes_tracked_wallets_as_explicit(monkeypatch, tmp_path) -> None:
    class FakeClickHouse:
        def __init__(self, _settings):
            pass

        def query_text(self, query):
            assert "mart_wallet_trade_rollup" in query
            return '{"user_address":"0xaaa0000000000000000000000000000000000000"}\n'

    class FakeTrackedWalletStore:
        def __init__(self, **_kwargs):
            pass

        def tracked_addresses(self):
            return ["0xbbb0000000000000000000000000000000000000"]

    monkeypatch.setattr("zetta.cli.ClickHouseWriter", FakeClickHouse)
    monkeypatch.setattr("zetta.cli.TrackedWalletStore", FakeTrackedWalletStore)
    args = type(
        "Args",
        (),
        {
            "task_store": "local",
            "task_file": str(tmp_path / "tasks.json"),
            "node_id": "node-1",
            "lease_seconds": 300,
            "wallet_limit": 10,
            "since_hours": 48,
            "page_limit": 500,
            "max_pages": 2,
            "candidate_mode": "recent",
            "min_notional": 10_000.0,
            "include_trades": True,
            "include_activity": False,
            "include_wallet_portfolio": False,
            "include_wallet_pnl": True,
            "include_tracked_wallets": True,
            "refresh_run": "realtime-wallets",
            "requeue_done": True,
            "wallets": [],
        },
    )()

    result = cmd_tasks_seed_wallets(args, Settings())
    tasks = LocalTaskStore(tmp_path / "tasks.json").load()

    assert result["wallets"] == 2
    assert result["explicit_wallets"] == 1
    assert {task.params["user"] for task in tasks} == {
        "0xaaa0000000000000000000000000000000000000",
        "0xbbb0000000000000000000000000000000000000",
    }
    assert {
        task.priority
        for task in tasks
        if task.params["user"] == "0xbbb0000000000000000000000000000000000000"
    } == {WALLET_EXPLICIT_REFRESH_PRIORITY}


def test_seed_active_event_wallets_adds_market_and_wallet_tasks(monkeypatch, tmp_path) -> None:
    queries = []

    class FakeClickHouse:
        def __init__(self, _settings):
            pass

        def query_text(self, query):
            queries.append(query)
            assert "active = true" in query
            assert "closed = false" in query
            assert "archived = false" in query
            if "from fact_trade_by_time" in query:
                assert "timestamp >= now64(3) - interval 90 minute" in query
                assert "sum(abs(notional)) >= 250.0" in query
                return '{"user_address":"0xabc"}\n{"user_address":"0xdef"}\n'
            if "select distinct condition_id" in query:
                return '{"condition_id":"condition-1"}\n{"condition_id":"condition-2"}\n'
            return ""

    monkeypatch.setattr("zetta.cli.ClickHouseWriter", FakeClickHouse)
    args = type(
        "Args",
        (),
        {
            "task_store": "local",
            "task_file": str(tmp_path / "tasks.json"),
            "node_id": "node-1",
            "lease_seconds": 300,
            "wallets": [],
            "event_limit": 10,
            "condition_limit": 25,
            "wallet_limit": 10,
            "since_minutes": 90,
            "min_notional": 250.0,
            "trade_page_limit": 500,
            "trade_max_pages": 1,
            "wallet_page_limit": 500,
            "wallet_max_pages": 1,
            "include_market_trades": True,
            "include_wallet_trades": True,
            "include_wallet_activity": True,
            "include_wallet_portfolio": True,
            "include_wallet_pnl": True,
            "include_unusual_betting_refresh": False,
            "unusual_betting_event_limit": 20,
            "unusual_betting_min_notional": 500000.0,
            "include_tracked_wallets": False,
            "refresh_run": "active-event-wallets",
            "requeue_done": True,
        },
    )()

    result = cmd_tasks_seed_active_event_wallets(args, Settings())
    tasks = LocalTaskStore(tmp_path / "tasks.json").load()

    assert len(queries) == 2
    assert result["condition_ids"] == 2
    assert result["wallets"] == 2
    assert result["candidate_tasks"] == 10
    assert result["added"] == 10
    assert [task.kind for task in tasks].count("trades") == 2
    assert [task.kind for task in tasks].count("wallet-trades") == 2
    assert [task.kind for task in tasks].count("wallet-activity") == 2
    assert [task.kind for task in tasks].count("wallet-portfolio") == 2
    assert [task.kind for task in tasks].count("wallet-pnl") == 2
    assert {task.priority for task in tasks if task.kind == "trades"} == {
        FRONTIER_TRADES_PRIORITY
    }
    assert {task.priority for task in tasks if task.kind in {"wallet-trades", "wallet-activity"}} == {
        ACTIVE_EVENT_WALLET_PRIORITY
    }
    assert all(task.params["_refresh_run"] == "active-event-wallets" for task in tasks)
    assert all(requeue_done_task(task.params) for task in tasks)


def test_seed_active_event_wallets_treats_tracked_wallets_as_explicit(
    monkeypatch, tmp_path
) -> None:
    class FakeClickHouse:
        def __init__(self, _settings):
            pass

        def query_text(self, query):
            assert "from fact_trade_by_time" in query
            return '{"user_address":"0xabc"}\n'

    class FakeTrackedWalletStore:
        def __init__(self, **_kwargs):
            pass

        def tracked_addresses(self):
            return ["0xDEF"]

    monkeypatch.setattr("zetta.cli.ClickHouseWriter", FakeClickHouse)
    monkeypatch.setattr("zetta.cli.TrackedWalletStore", FakeTrackedWalletStore)
    args = type(
        "Args",
        (),
        {
            "task_store": "local",
            "task_file": str(tmp_path / "tasks.json"),
            "node_id": "node-1",
            "lease_seconds": 300,
            "wallets": [],
            "event_limit": 0,
            "condition_limit": 25,
            "wallet_limit": 10,
            "since_minutes": 90,
            "min_notional": 0.0,
            "trade_page_limit": 500,
            "trade_max_pages": 1,
            "wallet_page_limit": 500,
            "wallet_max_pages": 1,
            "include_market_trades": False,
            "include_wallet_trades": False,
            "include_wallet_activity": False,
            "include_wallet_portfolio": True,
            "include_wallet_pnl": True,
            "include_unusual_betting_refresh": False,
            "unusual_betting_event_limit": 20,
            "unusual_betting_min_notional": 500000.0,
            "include_tracked_wallets": True,
            "refresh_run": "active-event-wallets",
            "requeue_done": True,
        },
    )()

    result = cmd_tasks_seed_active_event_wallets(args, Settings())
    tasks = LocalTaskStore(tmp_path / "tasks.json").load()

    assert result["wallets"] == 2
    assert result["explicit_wallets"] == 1
    assert {task.params["user"] for task in tasks} == {"0xabc", "0xdef"}
    assert {
        task.priority
        for task in tasks
        if task.params["user"] == "0xdef"
    } == {WALLET_EXPLICIT_REFRESH_PRIORITY}


def test_seed_active_event_wallets_can_link_unusual_betting_refresh(
    monkeypatch, tmp_path
) -> None:
    class FakeClickHouse:
        def __init__(self, _settings):
            pass

        def query_text(self, query):
            assert "from fact_trade_by_time as trades" in query
            assert "having total_notional >= 500000.0" in query
            assert "limit 20" in query
            return (
                '{"event_slug":"fifwc-esp-cvi-2026-06-15-more-markets"}\n'
                '{"event_slug":"fifwc-fra-sen-2026-06-16"}\n'
            )
            return ""

    monkeypatch.setattr("zetta.cli.ClickHouseWriter", FakeClickHouse)
    args = type(
        "Args",
        (),
        {
            "task_store": "local",
            "task_file": str(tmp_path / "tasks.json"),
            "node_id": "node-1",
            "lease_seconds": 300,
            "wallets": [],
            "event_limit": 10,
            "condition_limit": 25,
            "wallet_limit": 0,
            "since_minutes": 90,
            "min_notional": 0.0,
            "trade_page_limit": 500,
            "trade_max_pages": 1,
            "wallet_page_limit": 500,
            "wallet_max_pages": 1,
            "include_market_trades": False,
            "include_wallet_trades": False,
            "include_wallet_activity": False,
            "include_wallet_portfolio": False,
            "include_wallet_pnl": False,
            "include_unusual_betting_refresh": True,
            "unusual_betting_event_limit": 20,
            "unusual_betting_min_notional": 500000.0,
            "include_tracked_wallets": False,
            "refresh_run": "active-event-wallets",
            "requeue_done": True,
        },
    )()

    result = cmd_tasks_seed_active_event_wallets(args, Settings())
    tasks = LocalTaskStore(tmp_path / "tasks.json").load()

    assert result["unusual_betting_events"] == 2
    assert [task.kind for task in tasks] == ["unusual-betting-refresh"] * 2
    assert {task.params["slug"] for task in tasks} == {
        "fifwc-esp-cvi-2026-06-15",
        "fifwc-fra-sen-2026-06-16",
    }
    assert all(task.params["trigger_reason"] == "active-event-wallets" for task in tasks)


def test_seed_unusual_betting_adds_refresh_tasks(monkeypatch, tmp_path) -> None:
    queries = []

    class FakeClickHouse:
        def __init__(self, _settings):
            pass

        def query_text(self, query):
            queries.append(query)
            if "from fact_exchange_fill" in query:
                return (
                    '{"event_slug":"fifwc-esp-cvi-2026-06-15-more-markets"}\n'
                    '{"event_slug":"fifwc-fra-sen-2026-06-16"}\n'
                )
            if "from dim_event final" in query:
                return (
                    '{"event_slug":"fifwc-esp-cvi-2026-06-15"}\n'
                    '{"event_slug":"fifwc-arg-alg-2026-06-16"}\n'
                )
            return ""

    monkeypatch.setattr("zetta.cli.ClickHouseWriter", FakeClickHouse)
    args = type(
        "Args",
        (),
        {
            "task_store": "local",
            "task_file": str(tmp_path / "tasks.json"),
            "node_id": "node-1",
            "lease_seconds": 300,
            "events": ["fifwc-fra-sen-2026-06-16-exact-score"],
            "event_limit": 10,
            "recent_limit": 10,
            "since_minutes": 30,
            "wallet_limit": 100,
            "trade_limit": 100,
            "cold_price_threshold": 0.25,
            "large_threshold": 500000.0,
            "very_large_threshold": 1000000.0,
            "extreme_threshold": 5000000.0,
            "include_related_markets": True,
            "refresh_run": "unusual-betting",
            "trigger_reason": "scheduled",
            "requeue_done": True,
        },
    )()

    result = cmd_tasks_seed_unusual_betting(args, Settings())
    tasks = LocalTaskStore(tmp_path / "tasks.json").load()

    assert len(queries) == 2
    assert result["events"] == 3
    assert result["candidate_tasks"] == 3
    assert result["added"] == 3
    assert [task.kind for task in tasks] == ["unusual-betting-refresh"] * 3
    assert {task.priority for task in tasks} == {UNUSUAL_BETTING_REFRESH_PRIORITY}
    assert {task.params["slug"] for task in tasks} == {
        "fifwc-esp-cvi-2026-06-15",
        "fifwc-fra-sen-2026-06-16",
        "fifwc-arg-alg-2026-06-16",
    }
    assert all(task.params["refresh"] is True for task in tasks)
    assert all(requeue_done_task(task.params) for task in tasks)


def test_local_task_store_retries_then_dead_letters(tmp_path) -> None:
    store = LocalTaskStore(tmp_path / "tasks.json")
    store.add_many([Task(kind="unknown", params={}, max_attempts=2)])
    runner = TaskRunner(
        settings=Settings(raw_data_dir=tmp_path / "raw", state_dir=tmp_path / "state"),
        task_store=store,
        node_id="test-node",
        run_store=LocalRunStore(tmp_path / "runs.jsonl"),
    )

    first = runner.run_once()
    second = runner.run_once()

    assert first["status"] == "retrying"
    assert second["status"] == "dead_lettered"
    assert store.summary()["dead_lettered"] == 1
    runs = [json.loads(line) for line in (tmp_path / "runs.jsonl").read_text().splitlines()]
    assert [run["status"] for run in runs] == ["retrying", "dead_lettered"]
    dead_letters = (tmp_path / "tasks.dead_letters.jsonl").read_text().splitlines()
    assert len(dead_letters) == 1


def test_task_runner_flushes_raw_writer_after_task(tmp_path) -> None:
    class FakeRunner(TaskRunner):
        def __init__(self, *, task_store):
            self.settings = Settings()
            self.task_store = task_store
            self.node_id = "test-node"
            self.run_store = LocalRunStore(tmp_path / "runs.jsonl")
            self.raw_writer = type("RawWriter", (), {"flushed": False})()
            self.raw_writer.flush = lambda: setattr(self.raw_writer, "flushed", True)

        def run_task(self, task):
            return {"items": 1}

    store = LocalTaskStore(tmp_path / "tasks.json")
    store.add_many([Task(kind="gamma-events", params={})])
    runner = FakeRunner(task_store=store)

    result = runner.run_once()

    assert result["status"] == "done"
    assert runner.raw_writer.flushed is True


def test_seed_history_adds_partitioned_tasks(monkeypatch, tmp_path) -> None:
    class FakeClickHouse:
        def __init__(self, _settings):
            pass

        def query_text(self, query):
            if "select distinct condition_id" in query:
                return '{"condition_id":"condition-1"}\n'
            if "select distinct token_id" in query:
                return '{"token_id":"token-1"}\n{"token_id":"token-2"}\n'
            return ""

    monkeypatch.setattr("zetta.cli.ClickHouseWriter", FakeClickHouse)
    args = type(
        "Args",
        (),
        {
            "task_store": "local",
            "task_file": str(tmp_path / "tasks.json"),
            "node_id": "node-1",
            "lease_seconds": 300,
            "event_limit": 10,
            "active_only": True,
            "include_trades": True,
            "include_price_history": True,
            "include_books": True,
            "include_chain_logs": True,
            "trade_page_limit": 500,
            "price_interval": "all",
            "price_fidelity": None,
            "chain_from_block": 100,
            "chain_to_block": 250,
            "chain_block_step": 100,
            "chain_addresses": ["0xabc"],
            "chain_topics": ["0xtopic"],
        },
    )()

    result = cmd_tasks_seed_history(args, Settings())
    tasks = LocalTaskStore(tmp_path / "tasks.json").load()

    assert result["candidate_tasks"] == 7
    assert result["added"] == 7
    assert [task.kind for task in tasks].count("trades") == 1
    assert [task.kind for task in tasks].count("prices-history") == 2
    assert [task.kind for task in tasks].count("book") == 2
    assert [task.kind for task in tasks].count("chain-logs") == 2
    chain_task = next(task for task in tasks if task.kind == "chain-logs")
    assert chain_task.params["addresses"] == ["0xabc"]


def test_seed_frontier_adds_event_refresh_tasks_by_default(monkeypatch, tmp_path) -> None:
    class FakeClickHouse:
        def __init__(self, _settings):
            pass

        def query_text(self, query):
            if "select event_id" in query:
                return '{"event_id":"event-1"}\n{"event_id":"event-2"}\n'
            return ""

    monkeypatch.setattr("zetta.cli.ClickHouseWriter", FakeClickHouse)
    args = type(
        "Args",
        (),
        {
            "task_store": "local",
            "task_file": str(tmp_path / "tasks.json"),
            "node_id": "node-1",
            "lease_seconds": 300,
            "event_limit": 10,
            "condition_limit": 2,
            "token_limit": 2,
            "sync_mode": "event",
            "include_gamma": True,
            "active_only": True,
            "include_trades": True,
            "include_price_history": True,
            "include_books": True,
            "gamma_page_limit": 100,
            "gamma_max_pages": 1,
            "gamma_resume": False,
            "gamma_sleep_seconds": 0.0,
            "trade_page_limit": 500,
            "trade_max_pages": 1,
            "price_interval": "1d",
            "price_fidelity": None,
            "holders_limit": 500,
            "positions_limit": 500,
            "include_holders": True,
            "include_market_positions": True,
            "include_open_interest": True,
            "refresh_run": "2026-06-08T00:00:00+00:00",
            "requeue_done": False,
            "include_global_trades": False,
        },
    )()

    result = cmd_tasks_seed_frontier(args, Settings())
    tasks = LocalTaskStore(tmp_path / "tasks.json").load()

    assert result["sync_mode"] == "event"
    assert result["event_ids"] == 2
    assert result["candidate_tasks"] == 4
    assert [task.kind for task in tasks].count("gamma-events") == 1
    assert [task.kind for task in tasks].count("gamma-markets") == 1
    assert [task.kind for task in tasks].count("event-refresh") == 2
    event_task = next(task for task in tasks if task.kind == "event-refresh")
    assert event_task.priority == FRONTIER_EVENT_PRIORITY
    assert event_task.params["refresh_run"] == args.refresh_run
    assert event_task.params["include_holders"] is True


def test_seed_frontier_can_add_typed_refreshable_tasks(monkeypatch, tmp_path) -> None:
    class FakeClickHouse:
        def __init__(self, _settings):
            pass

        def query_text(self, query):
            if "select distinct condition_id" in query:
                return '{"condition_id":"condition-1"}\n{"condition_id":"condition-2"}\n'
            if "select distinct token_id" in query:
                return '{"token_id":"token-1"}\n{"token_id":"token-2"}\n'
            return ""

    monkeypatch.setattr("zetta.cli.ClickHouseWriter", FakeClickHouse)
    args = type(
        "Args",
        (),
        {
            "task_store": "local",
            "task_file": str(tmp_path / "tasks.json"),
            "node_id": "node-1",
            "lease_seconds": 300,
            "event_limit": 10,
            "condition_limit": 2,
            "token_limit": 2,
            "sync_mode": "typed",
            "include_gamma": True,
            "active_only": True,
            "include_trades": True,
            "include_price_history": True,
            "include_books": True,
            "gamma_page_limit": 100,
            "gamma_max_pages": 1,
            "gamma_resume": False,
            "gamma_sleep_seconds": 0.0,
            "trade_page_limit": 500,
            "trade_max_pages": 1,
            "price_interval": "1d",
            "price_fidelity": None,
            "holders_limit": 500,
            "positions_limit": 500,
            "include_holders": True,
            "include_market_positions": True,
            "include_open_interest": True,
            "refresh_run": "2026-06-08T00:00:00+00:00",
            "requeue_done": False,
            "include_global_trades": False,
        },
    )()

    result = cmd_tasks_seed_frontier(args, Settings())
    duplicate = cmd_tasks_seed_frontier(args, Settings())
    tasks = LocalTaskStore(tmp_path / "tasks.json").load()

    assert result["candidate_tasks"] == 8
    assert result["added"] == 8
    assert duplicate["added"] == 0
    assert [task.kind for task in tasks].count("gamma-events") == 1
    assert [task.kind for task in tasks].count("gamma-markets") == 1
    assert [task.kind for task in tasks].count("trades") == 2
    assert [task.kind for task in tasks].count("prices-history") == 2
    assert [task.kind for task in tasks].count("book") == 2
    assert {task.priority for task in tasks if task.kind.startswith("gamma-")} == {
        FRONTIER_GAMMA_PRIORITY
    }
    assert {task.priority for task in tasks if task.kind == "trades"} == {
        FRONTIER_TRADES_PRIORITY
    }
    trade_task = next(task for task in tasks if task.kind == "trades")
    assert trade_task.params["resume"] is False
    assert {task.priority for task in tasks if task.kind == "prices-history"} == {
        FRONTIER_PRICE_HISTORY_PRIORITY
    }
    assert {task.priority for task in tasks if task.kind == "book"} == {FRONTIER_BOOK_PRIORITY}
    assert all(task.params["_refresh_run"] == args.refresh_run for task in tasks)


def test_seed_frontier_can_seed_trade_tasks_without_gamma(monkeypatch, tmp_path) -> None:
    class FakeClickHouse:
        def __init__(self, _settings):
            pass

        def query_text(self, query):
            if "select distinct condition_id" in query:
                return '{"condition_id":"condition-1"}\n{"condition_id":"condition-2"}\n'
            return ""

    monkeypatch.setattr("zetta.cli.ClickHouseWriter", FakeClickHouse)
    args = type(
        "Args",
        (),
        {
            "task_store": "local",
            "task_file": str(tmp_path / "tasks.json"),
            "node_id": "node-1",
            "lease_seconds": 300,
            "event_limit": 10,
            "condition_limit": 2,
            "token_limit": 0,
            "sync_mode": "typed",
            "include_gamma": False,
            "active_only": True,
            "include_trades": True,
            "include_price_history": False,
            "include_books": False,
            "gamma_page_limit": 100,
            "gamma_max_pages": 1,
            "gamma_resume": False,
            "gamma_sleep_seconds": 0.0,
            "trade_page_limit": 500,
            "trade_max_pages": 1,
            "price_interval": "1d",
            "price_fidelity": None,
            "holders_limit": 500,
            "positions_limit": 500,
            "include_holders": False,
            "include_market_positions": False,
            "include_open_interest": False,
            "refresh_run": "realtime-trades",
            "requeue_done": True,
            "include_global_trades": True,
        },
    )()

    result = cmd_tasks_seed_frontier(args, Settings())
    tasks = LocalTaskStore(tmp_path / "tasks.json").load()

    assert result["candidate_tasks"] == 3
    assert result["include_gamma"] is False
    assert {task.kind for task in tasks} == {"trades"}
    assert [task.params["market"] for task in tasks] == [None, "condition-1", "condition-2"]
    assert all(requeue_done_task(task.params) for task in tasks)


def test_local_task_store_requeues_done_realtime_task(tmp_path) -> None:
    store = LocalTaskStore(tmp_path / "tasks.json")
    task = Task(kind="trades", params={"market": "condition-1", "_requeue_done": True})
    assert store.add_many([task]) == 1
    store.complete(task.id)

    added = store.add_many(
        [Task(kind="trades", params={"market": "condition-1", "_requeue_done": True})]
    )
    tasks = store.load()

    assert added == 1
    assert len(tasks) == 1
    assert tasks[0].status == "pending"
    assert tasks[0].attempts == 0
