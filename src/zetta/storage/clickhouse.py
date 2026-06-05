from __future__ import annotations

import base64
import json
import re
from collections.abc import Sequence
from datetime import date, datetime
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from zetta.config import Settings


class ClickHouseUnavailable(RuntimeError):
    pass


class ClickHouseWriter:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.base_url = f"http://{settings.clickhouse_host}:{settings.clickhouse_port}/"
        token = f"{settings.clickhouse_user}:{settings.clickhouse_password}".encode("utf-8")
        self.auth_header = f"Basic {base64.b64encode(token).decode('ascii')}"

    def insert(self, table: str, rows: Sequence[dict[str, Any]]) -> int:
        if not rows:
            return 0
        query = f"INSERT INTO {table} FORMAT JSONEachRow"
        payload = "\n".join(json.dumps(row, default=json_default, ensure_ascii=False) for row in rows)
        request = Request(
            f"{self.base_url}?{urlencode({'database': self.settings.clickhouse_database, 'query': query})}",
            data=payload.encode("utf-8"),
            headers={
                "Authorization": self.auth_header,
                "Content-Type": "application/x-ndjson; charset=utf-8",
            },
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.settings.request_timeout_seconds) as response:
                response.read()
        except HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            raise ClickHouseUnavailable(
                f"ClickHouse insert into {table} failed with HTTP {exc.code}: {details}"
            ) from exc
        except Exception as exc:
            raise ClickHouseUnavailable(f"ClickHouse insert into {table} failed: {exc}") from exc
        return len(rows)

    def query_text(self, query: str) -> str:
        request = Request(
            f"{self.base_url}?{urlencode({'database': self.settings.clickhouse_database, 'query': query})}",
            headers={"Authorization": self.auth_header},
        )
        try:
            with urlopen(request, timeout=self.settings.request_timeout_seconds) as response:
                return response.read().decode("utf-8")
        except HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            raise ClickHouseUnavailable(
                f"ClickHouse query failed with HTTP {exc.code}: {details}"
            ) from exc
        except Exception as exc:
            raise ClickHouseUnavailable(f"ClickHouse query failed: {exc}") from exc

    def execute(self, query: str) -> str:
        request = Request(
            f"{self.base_url}?{urlencode({'database': self.settings.clickhouse_database, 'query': query})}",
            data=b"",
            headers={"Authorization": self.auth_header},
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.settings.request_timeout_seconds) as response:
                return response.read().decode("utf-8")
        except HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            raise ClickHouseUnavailable(
                f"ClickHouse execute failed with HTTP {exc.code}: {details}"
            ) from exc
        except Exception as exc:
            raise ClickHouseUnavailable(f"ClickHouse execute failed: {exc}") from exc

    def execute_statements(self, sql: str) -> int:
        statements = split_sql_statements(sql)
        for statement in statements:
            self.execute(statement)
        return len(statements)

    def ping(self) -> bool:
        request = Request(
            f"{self.base_url}?{urlencode({'database': self.settings.clickhouse_database, 'query': 'SELECT 1'})}",
            headers={"Authorization": self.auth_header},
        )
        try:
            with urlopen(request, timeout=self.settings.request_timeout_seconds) as response:
                return response.status == 200
        except Exception as exc:
            raise ClickHouseUnavailable(f"ClickHouse ping failed: {exc}") from exc


def split_sql_statements(sql: str) -> list[str]:
    without_comments = re.sub(r"^\s*--.*$", "", sql, flags=re.MULTILINE)
    return [statement.strip() for statement in without_comments.split(";") if statement.strip()]


def json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
