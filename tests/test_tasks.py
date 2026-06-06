from datetime import UTC, datetime
import json

from zetta.config import Settings
from zetta.cli import cmd_tasks_seed_history
from zetta.scheduler.runner import TaskRunner
from zetta.scheduler.tasks import LocalRunStore, LocalTaskStore, Task, row_to_task, task_source_entity


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


def test_task_source_entity_maps_known_task_kinds() -> None:
    assert task_source_entity("gamma-events") == ("gamma", "events")
    assert task_source_entity("prices-history") == ("clob", "prices_history")
    assert task_source_entity("market-positions") == ("data", "market_positions")
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
    task = row_to_task((123, "gamma-events", {"page_limit": 100}, "running", 2, now, now, None, 7))

    assert task.id == "123"
    assert task.kind == "gamma-events"
    assert task.params == {"page_limit": 100}
    assert task.status == "running"
    assert task.attempts == 2
    assert task.max_attempts == 7


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
