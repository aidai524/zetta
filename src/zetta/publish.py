from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs


SCHEMA_VERSION = 1
PUBLISH_FRESH_SECONDS = 180
PUBLISH_DEGRADED_SECONDS = 600
PUBLISH_STALE_SECONDS = 1800


@dataclass(frozen=True)
class PublishDatasetSpec:
    dataset: str
    path: str
    query: str
    list_key: str | None = None


@dataclass(frozen=True)
class PublishSnapshot:
    dataset: str
    version: str
    manifest: dict[str, Any]
    payload: Any


CORE_API_DATASETS: dict[str, PublishDatasetSpec] = {
    "wallets_screener_fifa": PublishDatasetSpec(
        dataset="wallets_screener_fifa",
        path="/wallets/screener",
        query="scope=fifa&mode=whale&limit=500",
        list_key="wallets",
    ),
    "wallets_polycop_fifa_signals": PublishDatasetSpec(
        dataset="wallets_polycop_fifa_signals",
        path="/wallets/polycop-fifa-signals",
        query="limit=500&min_fifa_notional=1000&data_quality=estimate",
        list_key="wallets",
    ),
    "wallets_fifa_24h_pnl": PublishDatasetSpec(
        dataset="wallets_fifa_24h_pnl",
        path="/wallets/fifa-24h-pnl",
        query="limit=500&sort=pnl_24h&direction=desc",
        list_key="wallets",
    ),
}


def publish_dataset_spec(dataset: str) -> PublishDatasetSpec:
    normalized = normalize_dataset_name(dataset)
    if normalized not in CORE_API_DATASETS:
        available = ", ".join(sorted(CORE_API_DATASETS))
        raise ValueError(f"unknown publish dataset {dataset!r}; available: {available}")
    return CORE_API_DATASETS[normalized]


def normalize_dataset_name(dataset: str) -> str:
    return str(dataset or "").strip().replace("-", "_")


def parse_query_string(query: str) -> dict[str, list[str]]:
    return parse_qs(query or "", keep_blank_values=True)


def api_datetime(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def version_timestamp(value: datetime | None = None) -> str:
    return (value or datetime.now(UTC)).astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def dataset_root(root: Path, dataset: str) -> Path:
    return root / normalize_dataset_name(dataset)


def current_pointer_path(root: Path, dataset: str) -> Path:
    return dataset_root(root, dataset) / "current"


def count_payload_rows(payload: Any, *, list_key: str | None = None) -> int:
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        if list_key and isinstance(payload.get(list_key), list):
            return len(payload[list_key])
        for key in ("wallets", "rows", "items", "trades", "positions"):
            value = payload.get(key)
            if isinstance(value, list):
                return len(value)
    return 1 if payload is not None else 0


def write_publish_snapshot(
    root: Path,
    dataset: str,
    payload: Any,
    *,
    list_key: str | None = None,
    version: str | None = None,
    source_env: str = "stg",
    generated_at: datetime | None = None,
    parameters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_dataset = normalize_dataset_name(dataset)
    generated_at = generated_at or datetime.now(UTC)
    version = version or version_timestamp(generated_at)
    root = Path(root)
    target_root = dataset_root(root, normalized_dataset)
    version_dir = target_root / version
    tmp_dir = target_root / f".{version}.tmp"
    target_root.mkdir(parents=True, exist_ok=True)
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True)

    data_bytes = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    checksum = f"sha256:{hashlib.sha256(data_bytes).hexdigest()}"
    row_count = count_payload_rows(payload, list_key=list_key)
    manifest = {
        "dataset": normalized_dataset,
        "version": version,
        "source_env": source_env,
        "generated_at": api_datetime(generated_at),
        "schema_version": SCHEMA_VERSION,
        "row_count": row_count,
        "checksum": checksum,
        "list_key": list_key or "",
        "parameters": parameters or {},
    }

    (tmp_dir / "data.json").write_bytes(data_bytes)
    (tmp_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    if version_dir.exists():
        shutil.rmtree(version_dir)
    tmp_dir.rename(version_dir)
    pointer_tmp = current_pointer_path(root, normalized_dataset).with_suffix(".tmp")
    pointer_tmp.write_text(version + "\n", encoding="utf-8")
    pointer_tmp.replace(current_pointer_path(root, normalized_dataset))
    return manifest


def load_publish_snapshot(root: Path, dataset: str) -> PublishSnapshot | None:
    normalized_dataset = normalize_dataset_name(dataset)
    pointer = current_pointer_path(Path(root), normalized_dataset)
    try:
        version = pointer.read_text(encoding="utf-8").strip()
    except Exception:
        return None
    if not version:
        return None
    version_dir = dataset_root(Path(root), normalized_dataset) / version
    try:
        manifest = json.loads((version_dir / "manifest.json").read_text(encoding="utf-8"))
        data_bytes = (version_dir / "data.json").read_bytes()
    except Exception:
        return None
    checksum = f"sha256:{hashlib.sha256(data_bytes).hexdigest()}"
    if manifest.get("checksum") and manifest.get("checksum") != checksum:
        return None
    try:
        payload = json.loads(data_bytes.decode("utf-8"))
    except Exception:
        return None
    return PublishSnapshot(
        dataset=normalized_dataset,
        version=str(manifest.get("version") or version),
        manifest=manifest,
        payload=payload,
    )


def publish_age_seconds(manifest: dict[str, Any], *, now: datetime | None = None) -> float | None:
    raw = str(manifest.get("generated_at") or "")
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return max(0.0, ((now or datetime.now(UTC)) - parsed.astimezone(UTC)).total_seconds())


def publish_freshness_status(
    age_seconds: float | None,
    *,
    fresh_seconds: int = PUBLISH_FRESH_SECONDS,
    degraded_seconds: int = PUBLISH_DEGRADED_SECONDS,
    stale_seconds: int = PUBLISH_STALE_SECONDS,
) -> str:
    if age_seconds is None:
        return "unknown"
    if age_seconds <= fresh_seconds:
        return "fresh"
    if age_seconds <= degraded_seconds:
        return "degraded"
    if age_seconds <= stale_seconds:
        return "stale"
    return "expired"
