from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass


class ProviderError(RuntimeError):
    """Raised when a fake provider fails."""


@dataclass(slots=True)
class ProviderResponse:
    provider: str
    text: str
    latency_ms: float
    input_tokens: int
    output_tokens: int
    estimated_cost: float


class FakeLLMProvider:
    """Deterministic-enough fake provider for local chaos tests.

    This avoids real API keys while still simulating latency, failures, and cost.
    """

    def __init__(
        self,
        name: str,
        fail_rate: float,
        base_latency_ms: int,
        cost_per_1k_tokens: float,
        rng: random.Random | None = None,
    ):
        # Injecting an RNG makes chaos runs reproducible; default keeps the
        # original module-level random behaviour.
        self._rng: random.Random = rng if rng is not None else random.Random()
        # Under concurrent load the same provider object is shared by every
        # worker thread.  random.Random is not thread-safe, so serialise the
        # draws - but never hold the lock across time.sleep(), which would
        # serialise the whole load test and defeat the point of concurrency.
        self._rng_lock = threading.Lock()
        self.name = name
        self.fail_rate = fail_rate
        self.base_latency_ms = base_latency_ms
        self.cost_per_1k_tokens = cost_per_1k_tokens

    def complete(self, prompt: str) -> ProviderResponse:
        start = time.perf_counter()
        with self._rng_lock:
            jitter_ms = self._rng.randint(0, 60)
        time.sleep((self.base_latency_ms + jitter_ms) / 1000.0)
        with self._rng_lock:
            failed = self._rng.random() < self.fail_rate
        if failed:
            raise ProviderError(f"{self.name} simulated failure")
        input_tokens = max(1, len(prompt.split()))
        with self._rng_lock:
            output_tokens = self._rng.randint(20, 80)
        cost = (input_tokens + output_tokens) / 1000.0 * self.cost_per_1k_tokens
        latency_ms = (time.perf_counter() - start) * 1000
        return ProviderResponse(
            provider=self.name,
            text=f"[{self.name}] reliable answer for: {prompt[:60]}",
            latency_ms=latency_ms,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            estimated_cost=cost,
        )
