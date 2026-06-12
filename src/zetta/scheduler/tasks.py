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
    priority: int = 100
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
                existing_task = next(
                    (
                        item
                        for item in tasks
                        if item.kind == task.kind and _stable_json(item.params) == key[1]
                    ),
                    None,
                )
                if (
                    existing_task is not None
                    and requeue_done_task(task.params)
                    and existing_task.status == "done"
                ):
                    existing_task.status = "pending"
                    existing_task.priority = task.priority
                    existing_task.attempts = 0
                    existing_task.last_error = None
                    existing_task.updated_at = datetime.now(UTC).isoformat()
                    added += 1
                continue
            tasks.append(task)
            existing.add(key)
            added += 1
        self.save(tasks)
        return added

    def claim_next(self) -> Task | None:
        tasks = self.load()
        pending = [
            (index, task)
            for index, task in enumerate(tasks)
            if task.status == "pending"
        ]
        if not pending:
            return None
        _index, task = min(pending, key=lambda item: (item[1].priority, item[0]))
        task.mark("running")
        self.save(tasks)
        return task

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

    def progress(self, *, recent_limit: int = 10) -> dict[str, Any]:
        tasks = self.load()
        return task_progress_from_rows(
            [
                {
                    "kind": task.kind,
                    "status": task.status,
                    "attempts": task.attempts,
                    "updated_at": task.updated_at,
                }
                for task in tasks
            ],
            recent_limit=recent_limit,
        )

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
                    task_values = (
                        task.kind,
                        source,
                        entity,
                        Jsonb(task.params),
                        task.status,
                        task.priority,
                        task.attempts,
                        task.max_attempts,
                        task.last_error,
                    )
                    cursor.execute(
                        """
                        insert into collector_tasks
                          (
                            task_type, source, entity, params, status, priority,
                            attempts, max_attempts, last_error
                          )
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        on conflict do nothing
                        """,
                        task_values,
                    )
                    changed = cursor.rowcount
                    if changed == 0 and requeue_done_task(task.params):
                        cursor.execute(
                            """
                            update collector_tasks
                            set status = 'pending',
                                priority = %s,
                                attempts = 0,
                                max_attempts = %s,
                                lease_owner = null,
                                lease_expires_at = null,
                                last_error = null,
                                updated_at = now()
                            where task_type = %s
                              and source = %s
                              and entity = %s
                              and md5(params::text) = md5(%s::jsonb::text)
                              and status = 'done'
                            """,
                            (
                                task.priority,
                                task.max_attempts,
                                task.kind,
                                source,
                                entity,
                                Jsonb(task.params),
                            ),
                        )
                        changed = cursor.rowcount
                    added += changed
        return added

    def claim_next(self) -> Task | None:
        psycopg, _Jsonb = import_psycopg()

        lease_expires_at = datetime.now(UTC) + timedelta(seconds=self.lease_seconds)
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cursor:
                row = self._claim_with_status(
                    cursor,
                    where_sql="status = 'pending'",
                    lease_expires_at=lease_expires_at,
                )
                if row is None:
                    row = self._claim_with_status(
                        cursor,
                        where_sql="status = 'running' and lease_expires_at < now()",
                        lease_expires_at=lease_expires_at,
                    )
        return row_to_task(row) if row else None

    def _claim_with_status(self, cursor, *, where_sql: str, lease_expires_at: datetime):
        cursor.execute(
            f"""
            with candidate as
            (
              select id
              from collector_tasks
              where {where_sql}
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
              task.created_at, task.updated_at, task.last_error, task.max_attempts, task.priority
            """,
            (self.node_id, lease_expires_at),
        )
        return cursor.fetchone()

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

    def progress(self, *, recent_limit: int = 10) -> dict[str, Any]:
        psycopg, _Jsonb = import_psycopg()

        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    select task_type, status, attempts, updated_at
                    from collector_tasks
                    """
                )
                task_rows = [
                    {
                        "kind": str(kind),
                        "status": str(status),
                        "attempts": int(attempts or 0),
                        "updated_at": updated_at.isoformat()
                        if hasattr(updated_at, "isoformat")
                        else str(updated_at),
                    }
                    for kind, status, attempts, updated_at in cursor.fetchall()
                ]

                cursor.execute(
                    """
                    select
                      r.task_id,
                      coalesce(t.task_type, '') as task_type,
                      r.node_id,
                      r.started_at,
                      r.finished_at,
                      r.status,
                      r.pages,
                      r.items,
                      extract(epoch from (coalesce(r.finished_at, now()) - r.started_at)),
                      r.error
                    from collector_runs as r
                    left join collector_tasks as t on t.id = r.task_id
                    order by r.started_at desc
                    limit %s
                    """,
                    (recent_limit,),
                )
                recent_runs = [
                    {
                        "task_id": str(task_id),
                        "kind": str(kind),
                        "node_id": str(node_id),
                        "started_at": started_at.isoformat()
                        if hasattr(started_at, "isoformat")
                        else str(started_at),
                        "finished_at": finished_at.isoformat()
                        if hasattr(finished_at, "isoformat")
                        else None,
                        "status": str(status),
                        "pages": int(pages or 0),
                        "items": int(items or 0),
                        "duration_seconds": round(float(duration_seconds or 0), 3),
                        "error": str(error) if error else None,
                    }
                    for (
                        task_id,
                        kind,
                        node_id,
                        started_at,
                        finished_at,
                        status,
                        pages,
                        items,
                        duration_seconds,
                        error,
                    ) in cursor.fetchall()
                ]

        progress = task_progress_from_rows(task_rows, recent_limit=recent_limit)
        progress["recent_runs"] = recent_runs
        return progress

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

    def record_event_sync(self, record: EventSyncRecord) -> None:
        psycopg, Jsonb = import_psycopg()

        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    insert into event_sync_runs
                      (
                        event_id, refresh_run, task_id, node_id, status, started_at,
                        finished_at, markets, condition_ids, token_ids, raw_paths,
                        error, details
                      )
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    on conflict (event_id, refresh_run) do update
                    set task_id = excluded.task_id,
                        node_id = excluded.node_id,
                        status = excluded.status,
                        started_at = excluded.started_at,
                        finished_at = excluded.finished_at,
                        markets = excluded.markets,
                        condition_ids = excluded.condition_ids,
                        token_ids = excluded.token_ids,
                        raw_paths = excluded.raw_paths,
                        error = excluded.error,
                        details = excluded.details,
                        updated_at = now()
                    """,
                    (
                        record.event_id,
                        record.refresh_run,
                        int(record.task_id),
                        record.node_id,
                        record.status,
                        record.started_at,
                        record.finished_at,
                        record.markets,
                        record.condition_ids,
                        record.token_ids,
                        record.raw_paths,
                        record.error,
                        Jsonb(record.details),
                    ),
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


@dataclass(frozen=True)
class EventSyncRecord:
    event_id: str
    refresh_run: str
    task_id: str
    node_id: str
    status: str
    started_at: datetime
    finished_at: datetime
    markets: int = 0
    condition_ids: int = 0
    token_ids: int = 0
    raw_paths: list[str] = field(default_factory=list)
    error: str | None = None
    details: dict[str, Any] = field(default_factory=dict)


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


def task_progress_from_rows(
    rows: list[dict[str, Any]],
    *,
    recent_limit: int = 10,
) -> dict[str, Any]:
    summary = {"pending": 0, "running": 0, "done": 0, "failed": 0, "dead_lettered": 0}
    by_kind: dict[str, dict[str, Any]] = {}
    for row in rows:
        status = str(row["status"])
        kind = str(row["kind"])
        if status in summary:
            summary[status] += 1
        kind_counts = by_kind.setdefault(
            kind,
            {
                "pending": 0,
                "running": 0,
                "done": 0,
                "failed": 0,
                "dead_lettered": 0,
                "total": 0,
                "done_percent": 0.0,
            },
        )
        if status in summary:
            kind_counts[status] += 1
        kind_counts["total"] += 1

    total = sum(summary.values())
    completed = summary["done"] + summary["dead_lettered"]
    for counts in by_kind.values():
        kind_total = int(counts["total"])
        if kind_total:
            counts["done_percent"] = round((int(counts["done"]) / kind_total) * 100, 2)

    active_rows = [
        row
        for row in rows
        if str(row["status"]) in {"pending", "running", "failed", "dead_lettered"}
    ]
    active_rows.sort(key=lambda row: str(row.get("updated_at") or ""), reverse=True)

    return {
        "summary": summary,
        "total_tasks": total,
        "completed_tasks": completed,
        "done_percent": round((summary["done"] / total) * 100, 2) if total else 0.0,
        "closed_percent": round((completed / total) * 100, 2) if total else 0.0,
        "by_kind": dict(sorted(by_kind.items())),
        "active": active_rows[:recent_limit],
        "recent_runs": [],
    }


def task_source_entity(kind: str) -> tuple[str, str]:
    if kind == "event-refresh":
        return "event", "refresh"
    if kind.startswith("gamma-"):
        return "gamma", kind.removeprefix("gamma-").replace("-", "_")
    if kind in {
        "trades",
        "activity",
        "holders",
        "market-positions",
        "positions",
        "wallet-portfolio",
        "wallet-pnl",
        "open-interest",
    }:
        return "data", kind.replace("-", "_")
    if kind in {"prices-history", "book"}:
        return "clob", kind.replace("-", "_")
    if kind == "chain-logs":
        return "polygon", "logs"
    return "unknown", kind.replace("-", "_")


def requeue_done_task(params: dict[str, Any]) -> bool:
    return bool(params.get("_requeue_done"))


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
    priority = optional[1] if len(optional) > 1 else 100
    return Task(
        id=str(task_id),
        kind=str(kind),
        params=dict(params or {}),
        priority=int(priority or 100),
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
