from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import TypeVar

T = TypeVar("T")


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(RuntimeError):
    """Raised when a circuit is open and calls should fail fast."""


@dataclass(slots=True)
class CircuitBreaker:
    """Production-safe 3-state circuit breaker.

    States:
    - CLOSED:    calls pass through; consecutive failures are counted.
    - OPEN:      calls fail fast until ``reset_timeout_seconds`` elapses.
    - HALF_OPEN: a limited number of probe calls are allowed; ``success_threshold``
                 consecutive probe successes close the circuit, a single probe
                 failure re-opens it immediately (no retry storm).
    """

    name: str
    failure_threshold: int
    reset_timeout_seconds: float
    success_threshold: int = 1
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    opened_at: float | None = None
    transition_log: list[dict[str, str | float]] = field(default_factory=list)
    # Guards every read-modify-write of the state machine.  Without it, two
    # threads can both see failure_count == threshold-1 and both trip the
    # circuit, or a probe success can be lost to a concurrent failure.
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)
    # True while a HALF_OPEN probe is in flight.  Without this, N concurrent
    # workers all see state == HALF_OPEN and all get waved through, so a
    # flaky provider gets N probes instead of 1 and any single failure
    # re-opens the circuit - it can then never close again.  Sequential runs
    # are unaffected: there is only ever one request in flight anyway.
    _probe_in_flight: bool = field(default=False, repr=False, compare=False)

    def allow_request(self) -> bool:
        """Return whether a request should be attempted.

        CLOSED always allows.  HALF_OPEN allows exactly ONE probe at a time -
        further concurrent callers are denied so a recovering provider is not
        hit by a probe storm.  OPEN denies until ``reset_timeout_seconds`` has
        elapsed since ``opened_at``, then moves to HALF_OPEN and lets a probe in.
        """
        with self._lock:
            if self.state == CircuitState.CLOSED:
                return True
            if self.state == CircuitState.HALF_OPEN:
                if self._probe_in_flight:
                    return False
                self._probe_in_flight = True
                return True

            # state == OPEN — has the cool-down elapsed?
            if self.opened_at is None:
                # Defensive: an OPEN circuit with no timestamp is treated as expired.
                self._transition(CircuitState.HALF_OPEN, "reset_timeout_elapsed")
                self.success_count = 0
                self._probe_in_flight = True
                return True
            elapsed = time.monotonic() - self.opened_at
            if elapsed >= self.reset_timeout_seconds:
                self._transition(CircuitState.HALF_OPEN, "reset_timeout_elapsed")
                self.success_count = 0
                self._probe_in_flight = True
                return True
            return False

    def call(self, fn: Callable[..., T], *args: object, **kwargs: object) -> T:
        """Call ``fn`` through the circuit breaker.

        The gate check deliberately sits OUTSIDE the try block: a CircuitOpenError
        must not be recorded as yet another provider failure.
        """
        if not self.allow_request():
            raise CircuitOpenError(f"circuit '{self.name}' is open")
        try:
            result = fn(*args, **kwargs)
        except Exception:
            self.record_failure()
            raise
        self.record_success()
        return result

    def record_success(self) -> None:
        """Record a successful call and close the circuit if probes have proven healthy."""
        with self._lock:
            self.failure_count = 0
            self.success_count += 1
            self._probe_in_flight = False
            if (
                self.state == CircuitState.HALF_OPEN
                and self.success_count >= self.success_threshold
            ):
                self._transition(CircuitState.CLOSED, "probe_success")
                self.success_count = 0
                self.opened_at = None

    def record_failure(self) -> None:
        """Record a failed call and open the circuit when warranted.

        HALF_OPEN and threshold breaches are handled separately so the transition
        log keeps two distinct, greppable reasons.
        """
        with self._lock:
            self.failure_count += 1
            self.success_count = 0
            self._probe_in_flight = False
            if self.state == CircuitState.HALF_OPEN:
                # A failing probe re-opens immediately - do not wait for the threshold.
                self._transition(CircuitState.OPEN, "probe_failure")
                self.opened_at = time.monotonic()
            elif self.failure_count >= self.failure_threshold:
                self._transition(CircuitState.OPEN, "failure_threshold_reached")
                self.opened_at = time.monotonic()

    def _transition(self, new_state: CircuitState, reason: str) -> None:
        """Append to the transition log and switch state.

        Callers MUST already hold ``self._lock`` - this method never takes it,
        so it stays safe to call from inside the locked sections above.
        """
        if self.state == new_state:
            return
        self.transition_log.append(
            {"from": self.state.value, "to": new_state.value, "reason": reason, "ts": time.time()}
        )
        self.state = new_state
