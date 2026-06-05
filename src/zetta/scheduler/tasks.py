from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4


TaskStatus = Literal["pending", "running", "done", "failed", "dead_lettered"]


@dataclass
class Task:
    kind: str
    params: dict[str, Any]
    id: str = field(default_factory=lambda: uuid4().hex)
    status: TaskStatus = "pending"
    attempts: int = 0
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    last_error: str | None = None
    max_attempts: int = 3

    def mark(self, status: TaskStatus, error: str | None = None) -> None:
        self.status = status
        self.updated_at = datetime.now(UTC).isoformat()
        self.last_error = error
        if status == "running":
            self.attempts += 1


class LocalTaskStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.save([])

    def load(self) -> list[Task]:
        tasks = []
        for row in json.loads(self.path.read_text(encoding="utf-8")):
            tasks.append(Task(**row))
        return tasks

    def save(self, tasks: list[Task]) -> None:
        payload = [asdict(task) for task in tasks]
        self.path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    def add_many(self, new_tasks: list[Task]) -> int:
        tasks = self.load()
        existing = {(task.kind, _stable_json(task.params)) for task in tasks}
        added = 0
        for task in new_tasks:
            key = (task.kind, _stable_json(task.params))
            if key in existing:
                continue
            tasks.append(task)
            existing.add(key)
            added += 1
        self.save(tasks)
        return added

    def claim_next(self) -> Task | None:
        tasks = self.load()
        for task in tasks:
            if task.status == "pending":
                task.mark("running")
                self.save(tasks)
                return task
        return None

    def complete(self, task_id: str) -> None:
        self._mark(task_id, "done")

    def fail(self, task_id: str, error: str) -> None:
        self._mark(task_id, "failed", error=error)

    def retry(self, task_id: str, error: str) -> None:
        self._mark(task_id, "pending", error=error)

    def dead_letter(self, task_id: str, error: str) -> None:
        self._mark(task_id, "dead_lettered", error=error)
        self._record_dead_letter(task_id, error)

    def summary(self) -> dict[str, int]:
        counts = {"pending": 0, "running": 0, "done": 0, "failed": 0, "dead_lettered": 0}
        for task in self.load():
            counts[task.status] += 1
        return counts

    def _mark(self, task_id: str, status: TaskStatus, error: str | None = None) -> None:
        tasks = self.load()
        for task in tasks:
            if task.id == task_id:
                task.mark(status, error)
                self.save(tasks)
                return
        raise KeyError(f"Task not found: {task_id}")

    def _record_dead_letter(self, task_id: str, error: str) -> None:
        task = next((item for item in self.load() if item.id == task_id), None)
        payload = {
            "task_id": task_id,
            "kind": task.kind if task else "",
            "params": task.params if task else {},
            "attempts": task.attempts if task else 0,
            "error": error,
            "created_at": datetime.now(UTC).isoformat(),
        }
        path = self.path.with_suffix(".dead_letters.jsonl")
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True, ensure_ascii=False) + "\n")


class PostgresTaskStore:
    def __init__(self, *, dsn: str, node_id: str, lease_seconds: int = 300) -> None:
        self.dsn = dsn
        self.node_id = node_id
        self.lease_seconds = lease_seconds

    def add_many(self, new_tasks: list[Task]) -> int:
        psycopg, Jsonb = import_psycopg()

        added = 0
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cursor:
                for task in new_tasks:
                    source, entity = task_source_entity(task.kind)
                    cursor.execute(
                        """
                        insert into collector_tasks
                          (task_type, source, entity, params, status, attempts, max_attempts, last_error)
                        values (%s, %s, %s, %s, %s, %s, %s, %s)
                        on conflict do nothing
                        """,
                        (
                            task.kind,
                            source,
                            entity,
                            Jsonb(task.params),
                            task.status,
                            task.attempts,
                            task.max_attempts,
                            task.last_error,
                        ),
                    )
                    added += cursor.rowcount
        return added

    def claim_next(self) -> Task | None:
        psycopg, _Jsonb = import_psycopg()

        lease_expires_at = datetime.now(UTC) + timedelta(seconds=self.lease_seconds)
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    with candidate as
                    (
                      select id
                      from collector_tasks
                      where status = 'pending'
                         or (status = 'running' and lease_expires_at < now())
                      order by priority asc, id asc
                      limit 1
                      for update skip locked
                    )
                    update collector_tasks as task
                    set
                      status = 'running',
                      attempts = attempts + 1,
                      lease_owner = %s,
                      lease_expires_at = %s,
                      updated_at = now()
                    from candidate
                    where task.id = candidate.id
                    returning task.id, task.task_type, task.params, task.status, task.attempts,
                      task.created_at, task.updated_at, task.last_error, task.max_attempts
                    """,
                    (self.node_id, lease_expires_at),
                )
                row = cursor.fetchone()
        return row_to_task(row) if row else None

    def complete(self, task_id: str) -> None:
        self._mark(task_id, "done")

    def fail(self, task_id: str, error: str) -> None:
        self._mark(task_id, "failed", error=error)

    def retry(self, task_id: str, error: str) -> None:
        self._mark(task_id, "pending", error=error)

    def dead_letter(self, task_id: str, error: str) -> None:
        self._mark(task_id, "dead_lettered", error=error)
        self._record_dead_letter(task_id, error)

    def summary(self) -> dict[str, int]:
        psycopg, _Jsonb = import_psycopg()

        counts = {"pending": 0, "running": 0, "done": 0, "failed": 0, "dead_lettered": 0}
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute("select status, count(*) from collector_tasks group by status")
                for status, count in cursor.fetchall():
                    counts[str(status)] = int(count)
        return counts

    def _mark(self, task_id: str, status: TaskStatus, error: str | None = None) -> None:
        psycopg, _Jsonb = import_psycopg()

        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    update collector_tasks
                    set status = %s,
                        lease_owner = null,
                        lease_expires_at = null,
                        last_error = %s,
                        updated_at = now()
                    where id = %s
                    """,
                    (status, error, task_id),
                )
                if cursor.rowcount == 0:
                    raise KeyError(f"Task not found: {task_id}")

    def record_run(self, run: TaskRunRecord) -> None:
        psycopg, _Jsonb = import_psycopg()

        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    insert into collector_runs
                      (task_id, node_id, started_at, finished_at, status, pages, items, raw_paths, error)
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        int(run.task_id),
                        self.node_id,
                        run.started_at,
                        run.finished_at,
                        run.status,
                        run.pages,
                        run.items,
                        run.raw_paths,
                        run.error,
                    ),
                )

    def _record_dead_letter(self, task_id: str, error: str) -> None:
        psycopg, _Jsonb = import_psycopg()

        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    insert into collector_dead_letters
                      (task_id, task_type, source, entity, params, attempts, node_id, error)
                    select id, task_type, source, entity, params, attempts, %s, %s
                    from collector_tasks
                    where id = %s
                    """,
                    (self.node_id, error, task_id),
                )


@dataclass(frozen=True)
class TaskRunRecord:
    task_id: str
    kind: str
    node_id: str
    started_at: datetime
    finished_at: datetime
    status: str
    pages: int = 0
    items: int = 0
    raw_paths: list[str] = field(default_factory=list)
    error: str | None = None


class LocalRunStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def record_run(self, run: TaskRunRecord) -> None:
        payload = {
            **asdict(run),
            "started_at": run.started_at.isoformat(),
            "finished_at": run.finished_at.isoformat(),
        }
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True, ensure_ascii=False) + "\n")


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def task_source_entity(kind: str) -> tuple[str, str]:
    if kind.startswith("gamma-"):
        return "gamma", kind.removeprefix("gamma-").replace("-", "_")
    if kind in {"trades", "activity", "holders", "market-positions", "open-interest"}:
        return "data", kind.replace("-", "_")
    if kind in {"prices-history", "book"}:
        return "clob", kind.replace("-", "_")
    if kind == "chain-logs":
        return "polygon", "logs"
    return "unknown", kind.replace("-", "_")


def row_to_task(row: Any) -> Task:
    (
        task_id,
        kind,
        params,
        status,
        attempts,
        created_at,
        updated_at,
        last_error,
        *optional,
    ) = row
    max_attempts = optional[0] if optional else 3
    return Task(
        id=str(task_id),
        kind=str(kind),
        params=dict(params or {}),
        status=status,
        attempts=int(attempts),
        created_at=created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at),
        updated_at=updated_at.isoformat() if hasattr(updated_at, "isoformat") else str(updated_at),
        last_error=last_error,
        max_attempts=int(max_attempts or 3),
    )


def import_psycopg():
    try:
        import psycopg
        from psycopg.types.json import Jsonb
    except ImportError as exc:
        raise RuntimeError(
            "Postgres task store requires psycopg. Install project dependencies with `pip install -e .`."
        ) from exc
    return psycopg, Jsonb
