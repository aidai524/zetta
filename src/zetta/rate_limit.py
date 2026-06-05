from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field


@dataclass
class TokenBucket:
    rate_per_second: float
    burst: float
    tokens: float = 0.0
    updated_at: float = 0.0
    lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def __post_init__(self) -> None:
        self.tokens = self.burst
        self.updated_at = time.monotonic()

    def wait(self) -> None:
        while True:
            with self.lock:
                now = time.monotonic()
                elapsed = now - self.updated_at
                self.tokens = min(self.burst, self.tokens + elapsed * self.rate_per_second)
                self.updated_at = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                needed = (
                    (1 - self.tokens) / self.rate_per_second
                    if self.rate_per_second > 0
                    else 1
                )
            time.sleep(min(max(needed, 0.001), 1.0))


class RateLimiter:
    def __init__(self, buckets: dict[str, TokenBucket]) -> None:
        self.buckets = buckets

    def wait(self, family: str) -> None:
        bucket = self.buckets.get(family)
        if bucket is None:
            return
        bucket.wait()

    def wait_all(self, buckets: list[str]) -> None:
        for bucket_name in buckets:
            self.wait(bucket_name)


def default_rate_limiter() -> RateLimiter:
    return RateLimiter(
        {
            "gamma": TokenBucket(rate_per_second=5, burst=10),
            "gamma:events_keyset": TokenBucket(rate_per_second=3, burst=6),
            "gamma:markets_keyset": TokenBucket(rate_per_second=3, burst=6),
            "data": TokenBucket(rate_per_second=5, burst=10),
            "data:activity": TokenBucket(rate_per_second=3, burst=6),
            "data:holders": TokenBucket(rate_per_second=3, burst=6),
            "data:market_positions": TokenBucket(rate_per_second=3, burst=6),
            "data:open_interest": TokenBucket(rate_per_second=4, burst=8),
            "data:trades": TokenBucket(rate_per_second=4, burst=8),
            "clob": TokenBucket(rate_per_second=4, burst=8),
            "clob:book": TokenBucket(rate_per_second=2, burst=4),
            "clob:prices_history": TokenBucket(rate_per_second=3, burst=6),
            "polygon": TokenBucket(rate_per_second=8, burst=16),
            "polygon:rpc": TokenBucket(rate_per_second=8, burst=16),
        }
    )


_GLOBAL_RATE_LIMITER: RateLimiter | None = None
_GLOBAL_LOCK = threading.Lock()


def global_rate_limiter() -> RateLimiter:
    global _GLOBAL_RATE_LIMITER
    with _GLOBAL_LOCK:
        if _GLOBAL_RATE_LIMITER is None:
            _GLOBAL_RATE_LIMITER = default_rate_limiter()
        return _GLOBAL_RATE_LIMITER
