from __future__ import annotations

from pathlib import Path

from zetta.storage.state import LocalStateStore


def is_raw_locator(value: str) -> bool:
    return value.endswith(".jsonl.gz") or ".jsonl.gz#" in value


def loaded_payload_hashes(clickhouse, *, source: str, entity: str) -> set[str]:
    try:
        output = clickhouse.query_text(
            "SELECT payload_hash FROM raw_ingest_log "
            f"WHERE source = '{source}' AND entity = '{entity}' FORMAT TSV"
        )
    except Exception:
        return set()
    return {line.strip() for line in output.splitlines() if line.strip()}


def loaded_payload_hashes_for_paths(
    clickhouse,
    *,
    source: str,
    entity: str,
    paths: list[str],
    chunk_size: int = 1000,
) -> set[str]:
    if not paths:
        return set()
    loaded: set[str] = set()
    for start in range(0, len(paths), chunk_size):
        clauses = [
            f"(raw_path = {sql_string(path)} OR startsWith(raw_path, {sql_string(path + '#')}))"
            for path in paths[start : start + chunk_size]
        ]
        try:
            output = clickhouse.query_text(
                "SELECT payload_hash FROM raw_ingest_log "
                f"WHERE source = '{source}' AND entity = '{entity}' "
                f"AND ({' OR '.join(clauses)}) FORMAT TSV"
            )
        except Exception:
            continue
        loaded.update(line.strip() for line in output.splitlines() if line.strip())
    return loaded


def loaded_max_raw_path(clickhouse, *, source: str, entity: str) -> str | None:
    try:
        output = clickhouse.query_text(
            "SELECT max(raw_path) FROM raw_ingest_log "
            f"WHERE source = '{source}' AND entity = '{entity}' FORMAT TSV"
        )
    except Exception:
        return None
    value = output.strip()
    if not value or not is_raw_locator(value):
        return None
    return value


def clickhouse_state_dir(clickhouse) -> Path | None:
    settings = getattr(clickhouse, "settings", None)
    state_dir = getattr(settings, "state_dir", None)
    return Path(state_dir) if state_dir is not None else None


def loader_checkpoint_raw_path(
    state_dir: Path | None,
    *,
    source: str,
    entity: str,
) -> str | None:
    if state_dir is None:
        return None
    try:
        value = LocalStateStore(state_dir).get(checkpoint_key(source, entity), {})
    except Exception:
        return None
    raw_path = value.get("raw_path") if isinstance(value, dict) else None
    return raw_path if isinstance(raw_path, str) and is_raw_locator(raw_path) else None


def save_loader_checkpoint_raw_path(
    state_dir: Path | None,
    *,
    source: str,
    entity: str,
    raw_path: str,
) -> None:
    if state_dir is None or not is_raw_locator(raw_path):
        return
    LocalStateStore(state_dir).set(checkpoint_key(source, entity), {"raw_path": raw_path})


def checkpoint_key(source: str, entity: str) -> str:
    return f"loader_checkpoints/{source}_{entity}"


def sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
