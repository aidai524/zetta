import subprocess
from http.client import IncompleteRead, RemoteDisconnected
from urllib.error import URLError

from zetta.http import (
    JsonHttpClient,
    api_buckets,
    api_endpoint,
    api_family,
    curl_resolve_args,
    parse_resolve_overrides,
)
from zetta.rate_limit import RateLimiter


def test_get_with_curl_parses_json_response(monkeypatch) -> None:
    calls = []

    def fake_run(args, **kwargs):
        calls.append((args, kwargs))
        return subprocess.CompletedProcess(args=args, returncode=0, stdout='{"ok":true}', stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    client = JsonHttpClient(timeout_seconds=5, user_agent="TestAgent/1")

    response = client._get_with_curl("https://example.test/data", URLError("boom"))

    assert response.status == 200
    assert response.body == {"ok": True}
    assert calls[0][0][0] == "curl"
    assert "TestAgent/1" in calls[0][0]


def test_curl_fallback_adds_resolve_override(monkeypatch) -> None:
    calls = []

    def fake_run(args, **kwargs):
        calls.append((args, kwargs))
        return subprocess.CompletedProcess(args=args, returncode=0, stdout='{"ok":true}', stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    client = JsonHttpClient(
        timeout_seconds=5,
        user_agent="TestAgent/1",
        resolve_overrides={"gamma-api.polymarket.com": "128.242.240.125"},
    )

    client._get_with_curl(
        "https://gamma-api.polymarket.com/events/keyset?limit=1",
        URLError("boom"),
    )

    assert "--resolve" in calls[0][0]
    assert "gamma-api.polymarket.com:443:128.242.240.125" in calls[0][0]


def test_api_family_and_endpoint_bucket_mapping() -> None:
    url = "https://gamma-api.polymarket.com/events/keyset?limit=1"

    assert api_family(url) == "gamma"
    assert api_endpoint(url) == "events_keyset"
    assert api_buckets(url) == ["gamma", "gamma:events_keyset"]

    assert api_buckets("https://data-api.polymarket.com/v1/market-positions") == [
        "data",
        "data:market_positions",
    ]
    assert api_buckets("https://clob.polymarket.com/book?token_id=1") == [
        "clob",
        "clob:book",
    ]


def test_parse_resolve_overrides_and_curl_args() -> None:
    overrides = parse_resolve_overrides(
        "gamma-api.polymarket.com:128.242.240.125, clob.polymarket.com:185.60.216.50"
    )

    assert overrides["gamma-api.polymarket.com"] == "128.242.240.125"
    assert curl_resolve_args("https://clob.polymarket.com/book", overrides) == [
        "--resolve",
        "clob.polymarket.com:443:185.60.216.50",
    ]


def test_http_client_waits_for_family_and_endpoint_buckets(monkeypatch) -> None:
    waited = []

    def fake_wait_all(self, buckets):
        waited.extend(buckets)

    monkeypatch.setattr(RateLimiter, "wait_all", fake_wait_all)
    monkeypatch.setattr(JsonHttpClient, "_get_with_curl", lambda self, full_url, exc: None)
    monkeypatch.setattr("zetta.http.urlopen", lambda *args, **kwargs: (_ for _ in ()).throw(URLError("x")))
    client = JsonHttpClient(
        timeout_seconds=1,
        user_agent="TestAgent/1",
        max_retries=0,
        rate_limiter=RateLimiter({}),
    )

    client.get("https://data-api.polymarket.com/trades", {"limit": 1})

    assert waited == ["data", "data:trades"]


def test_http_client_retries_timeout_error_then_curl_fallback(monkeypatch) -> None:
    curl_calls = []

    monkeypatch.setattr("zetta.http.urlopen", lambda *args, **kwargs: (_ for _ in ()).throw(TimeoutError("slow")))
    monkeypatch.setattr("zetta.http.time.sleep", lambda _seconds: None)

    def fake_curl(self, full_url, exc):
        curl_calls.append((full_url, exc))
        return None

    monkeypatch.setattr(JsonHttpClient, "_get_with_curl", fake_curl)
    client = JsonHttpClient(timeout_seconds=1, user_agent="TestAgent/1", max_retries=1)

    client.get("https://gamma-api.polymarket.com/events/keyset", {"limit": 1})

    assert len(curl_calls) == 1
    assert isinstance(curl_calls[0][1], TimeoutError)


def test_http_client_retries_remote_disconnect_then_curl_fallback(monkeypatch) -> None:
    curl_calls = []

    monkeypatch.setattr(
        "zetta.http.urlopen",
        lambda *args, **kwargs: (_ for _ in ()).throw(RemoteDisconnected("closed")),
    )
    monkeypatch.setattr("zetta.http.time.sleep", lambda _seconds: None)

    def fake_curl(self, full_url, exc):
        curl_calls.append((full_url, exc))
        return None

    monkeypatch.setattr(JsonHttpClient, "_get_with_curl", fake_curl)
    client = JsonHttpClient(timeout_seconds=1, user_agent="TestAgent/1", max_retries=1)

    client.get("https://gamma-api.polymarket.com/events/keyset", {"limit": 1})

    assert len(curl_calls) == 1
    assert isinstance(curl_calls[0][1], RemoteDisconnected)


def test_http_client_retries_incomplete_read_then_curl_fallback(monkeypatch) -> None:
    curl_calls = []

    monkeypatch.setattr(
        "zetta.http.urlopen",
        lambda *args, **kwargs: (_ for _ in ()).throw(IncompleteRead(b"partial")),
    )
    monkeypatch.setattr("zetta.http.time.sleep", lambda _seconds: None)

    def fake_curl(self, full_url, exc):
        curl_calls.append((full_url, exc))
        return None

    monkeypatch.setattr(JsonHttpClient, "_get_with_curl", fake_curl)
    client = JsonHttpClient(timeout_seconds=1, user_agent="TestAgent/1", max_retries=1)

    client.get("https://gamma-api.polymarket.com/events/keyset", {"limit": 1})

    assert len(curl_calls) == 1
    assert isinstance(curl_calls[0][1], IncompleteRead)
