from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
import time
from typing import Any

from zetta.collectors.clob import ClobCollector
from zetta.collectors.chain import ChainCollector
from zetta.collectors.data import DataCollector
from zetta.collectors.gamma import GammaCollector
from zetta.config import Settings
from zetta.chain.rpc import PolygonRpcClient
from zetta.polymarket import PolymarketClient
from zetta.scheduler.tasks import Task, TaskRunRecord
from zetta.storage.raw import RawJsonlWriter
from zetta.storage.state import LocalStateStore


class TaskRunner:
    def __init__(
        self,
        *,
        settings: Settings,
        task_store,
        node_id: str = "local-node",
        run_store=None,
    ) -> None:
        self.settings = settings
        self.task_store = task_store
        self.node_id = node_id
        self.run_store = run_store if run_store is not None else task_store
        self.client = PolymarketClient(settings)
        self.raw_writer = RawJsonlWriter(
            settings.raw_data_dir,
            chunk_records=settings.raw_chunk_records,
            chunk_seconds=settings.raw_chunk_seconds,
        )
        self.state_store = LocalStateStore(settings.state_dir)

    def run_once(self) -> dict[str, Any]:
        task = self.task_store.claim_next()
        if task is None:
            return {"task": None, "status": "idle"}
        started_at = datetime.now(UTC)
        try:
            result = self.run_task(task)
            self.task_store.complete(task.id)
            finished_at = datetime.now(UTC)
            self.record_run(task, started_at, finished_at, "done", result=result)
            return {"task": task.id, "kind": task.kind, "status": "done", "result": result}
        except Exception as exc:
            finished_at = datetime.now(UTC)
            error = str(exc)
            if task.attempts >= task.max_attempts:
                mark_dead_letter(self.task_store, task.id, error)
                status = "dead_lettered"
            else:
                mark_retry(self.task_store, task.id, error)
                status = "retrying"
            self.record_run(task, started_at, finished_at, status, error=error)
            return {"task": task.id, "kind": task.kind, "status": status, "error": error}

    def run_loop(
        self,
        *,
        max_tasks: int = 0,
        idle_sleep_seconds: float = 5.0,
        stop_on_idle: bool = False,
    ) -> dict[str, Any]:
        completed = 0
        idle_cycles = 0
        last_result: dict[str, Any] | None = None
        while max_tasks == 0 or completed < max_tasks:
            last_result = self.run_once()
            if last_result["status"] == "idle":
                self.raw_writer.flush()
                idle_cycles += 1
                if stop_on_idle:
                    break
                time.sleep(idle_sleep_seconds)
                continue
            completed += 1
        return {
            "status": "stopped",
            "completed": completed,
            "idle_cycles": idle_cycles,
            "last_result": last_result,
        }

    def run_task(self, task: Task) -> dict[str, Any]:
        result: Any
        if task.kind == "gamma-events":
            result = GammaCollector(
                client=self.client,
                raw_writer=self.raw_writer,
                state_store=self.state_store,
            ).collect_keyset("events", **task.params)
        elif task.kind == "gamma-markets":
            result = GammaCollector(
                client=self.client,
                raw_writer=self.raw_writer,
                state_store=self.state_store,
            ).collect_keyset("markets", **task.params)
        elif task.kind == "trades":
            result = DataCollector(
                client=self.client,
                raw_writer=self.raw_writer,
                state_store=self.state_store,
            ).collect_trades(**task.params)
        elif task.kind == "prices-history":
            result = ClobCollector(
                client=self.client,
                raw_writer=self.raw_writer,
            ).collect_prices_history(**task.params)
        elif task.kind == "book":
            result = ClobCollector(
                client=self.client,
                raw_writer=self.raw_writer,
            ).collect_book(**task.params)
        elif task.kind == "chain-logs":
            result = ChainCollector(
                client=PolygonRpcClient(self.settings),
                raw_writer=self.raw_writer,
            ).collect_logs(**task.params)
        else:
            raise ValueError(f"Unknown task kind: {task.kind}")

        if is_dataclass(result):
            return asdict(result)
        return result

    def record_run(
        self,
        task: Task,
        started_at: datetime,
        finished_at: datetime,
        status: str,
        *,
        result: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        if not hasattr(self.run_store, "record_run"):
            return
        result = result or {}
        record = TaskRunRecord(
            task_id=task.id,
            kind=task.kind,
            node_id=self.node_id,
            started_at=started_at,
            finished_at=finished_at,
            status=status,
            pages=int(result.get("pages", 0) or 0),
            items=task_result_items(result),
            raw_paths=task_result_raw_paths(result),
            error=error,
        )
        self.run_store.record_run(record)


def mark_retry(task_store, task_id: str, error: str) -> None:
    if hasattr(task_store, "retry"):
        task_store.retry(task_id, error)
    else:
        task_store.fail(task_id, error)


def mark_dead_letter(task_store, task_id: str, error: str) -> None:
    if hasattr(task_store, "dead_letter"):
        task_store.dead_letter(task_id, error)
    else:
        task_store.fail(task_id, error)


def task_result_items(result: dict[str, Any]) -> int:
    for key in ("items", "trades", "logs"):
        value = result.get(key)
        if value is not None:
            return int(value or 0)
    return 0


def task_result_raw_paths(result: dict[str, Any]) -> list[str]:
    raw_paths = result.get("raw_paths")
    if isinstance(raw_paths, list):
        return [str(path) for path in raw_paths]
    output_path = result.get("output_path")
    return [str(output_path)] if output_path else []
