from __future__ import annotations

import json
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
from zetta.scheduler.tasks import EventSyncRecord, Task, TaskRunRecord
from zetta.storage.clickhouse import ClickHouseWriter
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
            finalized_paths = self.raw_writer.flush()
            result = normalize_result_raw_paths(result, finalized_paths)
            self.task_store.complete(task.id)
            finished_at = datetime.now(UTC)
            self.record_run(task, started_at, finished_at, "done", result=result)
            self.record_event_sync(task, started_at, finished_at, "done", result=result)
            return {"task": task.id, "kind": task.kind, "status": "done", "result": result}
        except Exception as exc:
            finalized_paths = self.raw_writer.flush()
            finished_at = datetime.now(UTC)
            error = str(exc)
            if task.attempts >= task.max_attempts:
                mark_dead_letter(self.task_store, task.id, error)
                status = "dead_lettered"
            else:
                mark_retry(self.task_store, task.id, error)
                status = "retrying"
            event_result = normalize_result_raw_paths(
                parse_event_refresh_error(error),
                finalized_paths,
            )
            self.record_run(task, started_at, finished_at, status, result=event_result, error=error)
            self.record_event_sync(
                task,
                started_at,
                finished_at,
                status,
                result=event_result,
                error=error,
            )
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
        params = task_execution_params(task.params)
        if task.kind == "gamma-events":
            result = GammaCollector(
                client=self.client,
                raw_writer=self.raw_writer,
                state_store=self.state_store,
            ).collect_keyset("events", **params)
        elif task.kind == "gamma-markets":
            result = GammaCollector(
                client=self.client,
                raw_writer=self.raw_writer,
                state_store=self.state_store,
            ).collect_keyset("markets", **params)
        elif task.kind == "trades":
            result = DataCollector(
                client=self.client,
                raw_writer=self.raw_writer,
                state_store=self.state_store,
            ).collect_trades(**params)
        elif task.kind == "activity":
            result = DataCollector(
                client=self.client,
                raw_writer=self.raw_writer,
                state_store=self.state_store,
            ).collect_activity(**params)
        elif task.kind == "wallet-portfolio":
            result = DataCollector(
                client=self.client,
                raw_writer=self.raw_writer,
                state_store=self.state_store,
                rpc_client=PolygonRpcClient(self.settings),
            ).collect_wallet_portfolio(**params)
        elif task.kind == "wallet-pnl":
            result = DataCollector(
                client=self.client,
                raw_writer=self.raw_writer,
                state_store=self.state_store,
            ).collect_user_pnl(**params)
        elif task.kind == "prices-history":
            result = ClobCollector(
                client=self.client,
                raw_writer=self.raw_writer,
            ).collect_prices_history(**params)
        elif task.kind == "book":
            result = ClobCollector(
                client=self.client,
                raw_writer=self.raw_writer,
            ).collect_book(**params)
        elif task.kind == "chain-logs":
            result = ChainCollector(
                client=PolygonRpcClient(self.settings),
                raw_writer=self.raw_writer,
            ).collect_logs(**params)
        elif task.kind == "event-refresh":
            result = self.refresh_event(task, **params)
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

    def refresh_event(
        self,
        task: Task,
        *,
        event_id: str,
        refresh_run: str,
        gamma_page_limit: int = 100,
        trade_page_limit: int = 500,
        trade_max_pages: int = 1,
        price_interval: str | None = "1d",
        price_fidelity: int | None = None,
        holders_limit: int = 500,
        positions_limit: int = 500,
        include_gamma: bool = False,
        include_holders: bool = True,
        include_market_positions: bool = True,
        include_open_interest: bool = True,
    ) -> dict[str, Any]:
        raw_paths: list[str] = []
        gamma = GammaCollector(
            client=self.client,
            raw_writer=self.raw_writer,
            state_store=self.state_store,
        )
        data = DataCollector(
            client=self.client,
            raw_writer=self.raw_writer,
            state_store=self.state_store,
        )
        clob = ClobCollector(client=self.client, raw_writer=self.raw_writer)

        if include_gamma:
            # Refresh Gamma dimensions first; loaders make these idempotent.
            for entity in ("events", "markets"):
                result = gamma.collect_keyset(
                    entity,
                    page_limit=gamma_page_limit,
                    max_pages=1,
                    resume=False,
                    closed=None,
                    archived=None,
                    active=None,
                )
                raw_paths.extend(result.raw_paths)

        event_ref = event_market_refs(ClickHouseWriter(self.settings), event_id=event_id)
        condition_ids = sorted({ref["condition_id"] for ref in event_ref if ref["condition_id"]})
        token_ids = sorted({token for ref in event_ref for token in ref["token_ids"] if token})

        items = 0
        pages = 0
        failures: list[dict[str, str]] = []

        for condition_id in condition_ids:
            try:
                result = data.collect_trades(
                    page_limit=trade_page_limit,
                    max_pages=trade_max_pages,
                    resume=False,
                    market=condition_id,
                )
                pages += result.pages
                items += result.trades
                raw_paths.extend(result.raw_paths)
            except Exception as exc:
                failures.append({"entity": "trades", "id": condition_id, "error": str(exc)})
            if include_holders:
                try:
                    result = data.collect_holders(market=condition_id, limit=holders_limit)
                    pages += result.pages
                    items += result.items
                    raw_paths.extend(result.raw_paths or [])
                except Exception as exc:
                    failures.append({"entity": "holders", "id": condition_id, "error": str(exc)})
            if include_market_positions:
                try:
                    result = data.collect_market_positions(
                        market=condition_id,
                        limit=positions_limit,
                    )
                    pages += result.pages
                    items += result.items
                    raw_paths.extend(result.raw_paths or [])
                except Exception as exc:
                    failures.append(
                        {"entity": "market_positions", "id": condition_id, "error": str(exc)}
                    )
            if include_open_interest:
                try:
                    result = data.collect_open_interest(market=condition_id)
                    pages += result.pages
                    items += result.items
                    raw_paths.extend(result.raw_paths or [])
                except Exception as exc:
                    failures.append({"entity": "open_interest", "id": condition_id, "error": str(exc)})

        for token_id in token_ids:
            try:
                result = clob.collect_prices_history(
                    token_id=token_id,
                    interval=price_interval,
                    fidelity=price_fidelity,
                )
                items += result.items
                if result.output_path:
                    raw_paths.append(result.output_path)
            except Exception as exc:
                failures.append({"entity": "prices_history", "id": token_id, "error": str(exc)})
            try:
                result = clob.collect_book(token_id=token_id)
                items += result.items
                if result.output_path:
                    raw_paths.append(result.output_path)
            except Exception as exc:
                failures.append({"entity": "book", "id": token_id, "error": str(exc)})

        result = {
            "event_id": event_id,
            "refresh_run": refresh_run,
            "markets": len(event_ref),
            "condition_ids": len(condition_ids),
            "token_ids": len(token_ids),
            "pages": pages,
            "items": items,
            "raw_paths": raw_paths,
            "failures": failures,
        }
        if failures:
            raise RuntimeError(json.dumps(result, sort_keys=True))
        return result

    def record_event_sync(
        self,
        task: Task,
        started_at: datetime,
        finished_at: datetime,
        status: str,
        *,
        result: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        if task.kind != "event-refresh" or not hasattr(self.run_store, "record_event_sync"):
            return
        result = result or parse_event_refresh_error(error)
        event_id = str(task.params.get("event_id") or result.get("event_id") or "")
        if not event_id:
            return
        refresh_run = str(task.params.get("refresh_run") or result.get("refresh_run") or "")
        self.run_store.record_event_sync(
            EventSyncRecord(
                event_id=event_id,
                refresh_run=refresh_run,
                task_id=task.id,
                node_id=self.node_id,
                status=status,
                started_at=started_at,
                finished_at=finished_at,
                markets=int(result.get("markets", 0) or 0),
                condition_ids=int(result.get("condition_ids", 0) or 0),
                token_ids=int(result.get("token_ids", 0) or 0),
                raw_paths=[str(path) for path in result.get("raw_paths", [])],
                error=error,
                details=result,
            )
        )


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


def task_execution_params(params: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in params.items() if not key.startswith("_")}


def normalize_result_raw_paths(result: dict[str, Any], finalized_paths: list[Any]) -> dict[str, Any]:
    if not isinstance(result, dict):
        return result
    normalized = dict(result)
    raw_paths = normalized.get("raw_paths")
    if isinstance(raw_paths, list):
        paths = [final_raw_path(str(path)) for path in raw_paths if path]
        paths.extend(final_raw_path(str(path)) for path in finalized_paths if path)
        normalized["raw_paths"] = list(dict.fromkeys(paths))
    output_path = normalized.get("output_path")
    if output_path:
        normalized["output_path"] = final_raw_path(str(output_path))
    return normalized


def final_raw_path(path: str) -> str:
    return path.removesuffix(".open")


def event_market_refs(clickhouse: ClickHouseWriter, *, event_id: str) -> list[dict[str, Any]]:
    sql = f"""
        select
          markets.market_id as market_id,
          markets.condition_id as condition_id,
          groupArrayDistinct(tokens.token_id) as token_ids
        from dim_market as markets final
        left join dim_outcome_token as tokens final on tokens.market_id = markets.market_id
        where markets.event_id = {ch_string(event_id)}
          and markets.market_id != ''
        group by markets.market_id, markets.condition_id
        order by markets.market_id
        format JSONEachRow
    """
    refs: list[dict[str, Any]] = []
    for line in clickhouse.query_text(sql).splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        refs.append(
            {
                "market_id": str(row.get("market_id") or ""),
                "condition_id": str(row.get("condition_id") or ""),
                "token_ids": [str(token) for token in row.get("token_ids") or [] if token],
            }
        )
    return refs


def parse_event_refresh_error(error: str | None) -> dict[str, Any]:
    if not error:
        return {}
    try:
        parsed = json.loads(error)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def ch_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
