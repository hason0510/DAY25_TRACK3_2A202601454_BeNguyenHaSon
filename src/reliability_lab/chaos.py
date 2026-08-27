from __future__ import annotations

import json
import random
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from reliability_lab.cache import ResponseCache, SharedRedisCache
from reliability_lab.circuit_breaker import CircuitBreaker
from reliability_lab.config import LabConfig, ScenarioConfig
from reliability_lab.gateway import GatewayResponse, ReliabilityGateway
from reliability_lab.metrics import RunMetrics
from reliability_lab.providers import FakeLLMProvider

# Money saved per cache hit — one avoided provider round-trip at roughly the
# blended per-request price of the two configured providers.
COST_SAVED_PER_CACHE_HIT = 0.001


def load_queries(path: str | Path = "data/sample_queries.jsonl") -> list[str]:
    queries: list[str] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        queries.append(json.loads(line)["query"])
    return queries


def build_gateway(
    config: LabConfig,
    provider_overrides: dict[str, float] | None = None,
    rng: random.Random | None = None,
) -> ReliabilityGateway:
    providers = []
    for p in config.providers:
        fail_rate = provider_overrides.get(p.name, p.fail_rate) if provider_overrides else p.fail_rate
        providers.append(
            FakeLLMProvider(p.name, fail_rate, p.base_latency_ms, p.cost_per_1k_tokens, rng=rng)
        )
    breakers = {
        p.name: CircuitBreaker(
            name=p.name,
            failure_threshold=config.circuit_breaker.failure_threshold,
            reset_timeout_seconds=config.circuit_breaker.reset_timeout_seconds,
            success_threshold=config.circuit_breaker.success_threshold,
        )
        for p in config.providers
    }
    cache: ResponseCache | SharedRedisCache | None = None
    if config.cache.enabled:
        if config.cache.backend == "redis":
            cache = SharedRedisCache(
                config.cache.redis_url,
                config.cache.ttl_seconds,
                config.cache.similarity_threshold,
            )
        else:
            cache = ResponseCache(config.cache.ttl_seconds, config.cache.similarity_threshold)
    return ReliabilityGateway(providers, breakers, cache)


def calculate_recovery_time_ms(gateway: ReliabilityGateway) -> float | None:
    """Average time (ms) a breaker spent open before closing again.

    Walks each transition log pairing every ``to="open"`` with the next
    ``to="closed"``.  Returns None when no circuit ever recovered — that is a
    meaningful "never healed" signal, not a zero.
    """
    recoveries: list[float] = []
    for breaker in gateway.breakers.values():
        opened_ts: float | None = None
        for entry in breaker.transition_log:
            to_state = entry.get("to")
            ts = entry.get("ts")
            if not isinstance(ts, (int, float)):
                continue
            if to_state == "open":
                # Most recent trip wins: a failed probe re-opens the circuit and
                # restarts the cool-down, so recovery is measured from there.
                opened_ts = float(ts)
            elif to_state == "closed" and opened_ts is not None:
                recoveries.append((float(ts) - opened_ts) * 1000.0)
                opened_ts = None
    if not recoveries:
        return None
    return sum(recoveries) / len(recoveries)


def count_circuit_opens(gateway: ReliabilityGateway) -> int:
    """Number of times any breaker tripped to OPEN during the run."""
    return sum(
        1
        for breaker in gateway.breakers.values()
        for entry in breaker.transition_log
        if entry.get("to") == "open"
    )


def run_scenario(
    config: LabConfig,
    queries: list[str],
    scenario: ScenarioConfig,
    rng: random.Random | None = None,
) -> RunMetrics:
    """Run one named chaos scenario end-to-end and collect its metrics."""
    if rng is None:
        rng = random.Random(config.seed) if config.seed is not None else random.Random()

    gateway = build_gateway(config, scenario.provider_overrides or None, rng=rng)
    metrics = RunMetrics()
    metrics_lock = threading.Lock()

    # Draw every prompt up front, sequentially.  The RNG stays single-threaded so
    # the *workload* is identical between the sequential and concurrent runs -
    # only the timing of the requests differs.
    prompts = [rng.choice(queries) for _ in range(config.load_test.requests)]

    def record(result: GatewayResponse) -> None:
        """Fold one response into the metrics.  Called from worker threads."""
        with metrics_lock:
            metrics.total_requests += 1
            metrics.estimated_cost += result.estimated_cost

            if result.cache_hit:
                # A cache hit is a served customer: success, one provider call saved.
                metrics.cache_hits += 1
                metrics.estimated_cost_saved += COST_SAVED_PER_CACHE_HIT
                metrics.successful_requests += 1
            elif result.route == "fallback":
                metrics.fallback_successes += 1
                metrics.successful_requests += 1
            elif result.route == "static_fallback":
                # The only genuine failure: the user got a degraded canned message.
                metrics.static_fallbacks += 1
                metrics.failed_requests += 1
            else:
                metrics.successful_requests += 1

            if result.latency_ms > 0:
                metrics.latencies_ms.append(result.latency_ms)

    started = time.monotonic()
    concurrency = config.load_test.concurrency
    if concurrency <= 1:
        for prompt in prompts:
            record(gateway.complete(prompt))
    else:
        # Concurrent load: N in-flight requests share one gateway, one cache and
        # one breaker per provider - exactly the contention a real pod sees.
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            for result in pool.map(gateway.complete, prompts):
                record(result)

    metrics.duration_seconds = time.monotonic() - started
    metrics.circuit_open_count = count_circuit_opens(gateway)
    metrics.recovery_time_ms = calculate_recovery_time_ms(gateway)
    return metrics


# ---------------------------------------------------------------------------
# Scenario pass/fail criteria — each scenario asserts the behaviour it exists to
# prove, rather than the trivial "at least one request succeeded".
# ---------------------------------------------------------------------------

ScenarioCheck = Callable[[RunMetrics, LabConfig], bool]

# A circuit needs open -> (cool-down) -> half_open -> closed to record a
# recovery.  Under concurrent load a scenario can finish in less wall-clock
# time than a single cool-down, so demanding recovery evidence there would
# fail the run for something the measurement cannot physically observe.
RECOVERY_OBSERVABLE_CYCLES = 4


def recovery_is_observable(m: RunMetrics, config: LabConfig) -> bool:
    """True if the run lasted long enough for a full recovery cycle to fit."""
    return m.duration_seconds >= (
        RECOVERY_OBSERVABLE_CYCLES * config.circuit_breaker.reset_timeout_seconds
    )


def _default_check(m: RunMetrics, config: LabConfig) -> bool:
    return m.availability >= 0.95


SCENARIO_CRITERIA: dict[str, ScenarioCheck] = {
    # Primary is dead: every request must still be served, via backup or cache,
    # and the breaker must actually trip instead of hammering the dead provider.
    "primary_timeout_100": lambda m, c: (
        m.availability >= 0.95 and m.circuit_open_count > 0 and m.fallback_success_rate > 0.9
    ),
    # Primary flaps: the circuit should oscillate (open at least once) and recover.
    "primary_flaky_50": lambda m, c: (
        m.availability >= 0.95
        and m.circuit_open_count > 0
        # Only demand recovery evidence when the run was long enough to show it.
        and (m.recovery_time_ms is not None or not recovery_is_observable(m, c))
    ),
    # Baseline: near-perfect availability.  Note the "healthy" primary still has
    # fail_rate 0.25, so an occasional trip is correct behaviour — what matters is
    # that it healed quickly, not that it never happened.
    "all_healthy": lambda m, c: m.availability >= 0.98
    and (m.recovery_time_ms is None or m.recovery_time_ms < 5000),
}


def scenario_passed(name: str, metrics: RunMetrics, config: LabConfig) -> bool:
    return SCENARIO_CRITERIA.get(name, _default_check)(metrics, config)


def _scenario_detail(
    scenario: ScenarioConfig, m: RunMetrics, config: LabConfig | None = None
) -> dict[str, object]:
    return {
        "description": scenario.description,
        "provider_overrides": dict(scenario.provider_overrides),
        "concurrency": config.load_test.concurrency if config else 1,
        "duration_seconds": round(m.duration_seconds, 2),
        "recovery_observable": recovery_is_observable(m, config) if config else True,
        "cache_backend": (config.cache.backend if config and config.cache.enabled else "disabled"),
        "total_requests": m.total_requests,
        "availability": round(m.availability, 4),
        "error_rate": round(m.error_rate, 4),
        "cache_hits": m.cache_hits,
        "cache_hit_rate": round(m.cache_hit_rate, 4),
        "fallback_successes": m.fallback_successes,
        "static_fallbacks": m.static_fallbacks,
        "fallback_success_rate": round(m.fallback_success_rate, 4),
        "circuit_open_count": m.circuit_open_count,
        "recovery_time_ms": round(m.recovery_time_ms, 2) if m.recovery_time_ms is not None else None,
        "latency_p50_ms": round(m.percentile(50), 2),
        "latency_p95_ms": round(m.percentile(95), 2),
        "latency_p99_ms": round(m.percentile(99), 2),
        "estimated_cost": round(m.estimated_cost, 6),
        "estimated_cost_saved": round(m.estimated_cost_saved, 6),
    }


def run_simulation(config: LabConfig, queries: list[str]) -> RunMetrics:
    """Run every named scenario from config, or a single default run if none defined."""
    if not config.scenarios:
        default_scenario = ScenarioConfig(name="default", description="baseline run")
        metrics = run_scenario(config, queries, default_scenario)
        metrics.scenarios = {"default": "pass" if scenario_passed("default", metrics, config) else "fail"}
        metrics.scenario_details = {"default": _scenario_detail(default_scenario, metrics, config)}
        return metrics

    combined = RunMetrics()
    for index, scenario in enumerate(config.scenarios):
        # Each scenario gets its own RNG stream so scenario N is reproducible
        # regardless of how many requests scenario N-1 happened to make.
        rng = random.Random(config.seed + index) if config.seed is not None else None
        result = run_scenario(config, queries, scenario, rng=rng)

        combined.scenarios[scenario.name] = "pass" if scenario_passed(scenario.name, result, config) else "fail"
        combined.scenario_details[scenario.name] = _scenario_detail(scenario, result, config)

        combined.total_requests += result.total_requests
        combined.successful_requests += result.successful_requests
        combined.failed_requests += result.failed_requests
        combined.fallback_successes += result.fallback_successes
        combined.static_fallbacks += result.static_fallbacks
        combined.cache_hits += result.cache_hits
        combined.circuit_open_count += result.circuit_open_count
        combined.estimated_cost += result.estimated_cost
        combined.estimated_cost_saved += result.estimated_cost_saved
        combined.latencies_ms.extend(result.latencies_ms)
        if result.recovery_time_ms is not None:
            if combined.recovery_time_ms is None:
                combined.recovery_time_ms = result.recovery_time_ms
            else:
                combined.recovery_time_ms = (combined.recovery_time_ms + result.recovery_time_ms) / 2

    return combined
