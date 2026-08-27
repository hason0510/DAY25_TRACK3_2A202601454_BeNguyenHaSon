from __future__ import annotations

import hashlib
import math
import re
import threading
import time
from collections import Counter
from dataclasses import dataclass
from typing import Any

# ---------------------------------------------------------------------------
# Shared utilities — use these in both ResponseCache and SharedRedisCache
# ---------------------------------------------------------------------------

PRIVACY_PATTERNS = re.compile(
    r"\b(balance|password|credit.card|ssn|social.security|user.\d+|account.\d+)\b",
    re.IGNORECASE,
)

# Word tokens are lowercased alphanumeric runs; each word also contributes its
# character 3-grams so that near-identical phrasings stay close in vector space.
_WORD_RE = re.compile(r"[a-z0-9]+")
_NGRAM_SIZE = 3


def _is_uncacheable(query: str) -> bool:
    """Return True if query contains privacy-sensitive keywords."""
    return bool(PRIVACY_PATTERNS.search(query))


def _looks_like_false_hit(query: str, cached_key: str) -> bool:
    """Return True if query and cached key contain different 4-digit numbers (years, IDs)."""
    nums_q = set(re.findall(r"\b\d{4}\b", query))
    nums_c = set(re.findall(r"\b\d{4}\b", cached_key))
    return bool(nums_q and nums_c and nums_q != nums_c)


def _tokenize(text: str) -> list[str]:
    """Split text into word tokens plus character n-grams inside each word.

    ``"hello"`` becomes ``["hello", "hel", "ell", "llo"]``.  Keeping both
    granularities means shared words score high while partial morphological
    overlap still contributes a graded signal instead of an all-or-nothing one.
    """
    tokens: list[str] = []
    for word in _WORD_RE.findall(text.lower()):
        tokens.append(word)
        for i in range(len(word) - _NGRAM_SIZE + 1):
            tokens.append(word[i : i + _NGRAM_SIZE])
    return tokens


# ---------------------------------------------------------------------------
# In-memory cache
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class CacheEntry:
    key: str
    value: str
    created_at: float
    metadata: dict[str, str]


class ResponseCache:
    """In-memory semantic cache with TTL, privacy guardrails and false-hit detection."""

    def __init__(self, ttl_seconds: int, similarity_threshold: float):
        self.ttl_seconds = ttl_seconds
        self.similarity_threshold = similarity_threshold
        self._entries: list[CacheEntry] = []
        self.false_hit_log: list[dict[str, object]] = []
        # get() rebinds self._entries during eviction; without a lock a
        # concurrent set() can append to the list that is about to be dropped.
        self._lock = threading.Lock()

    def get(self, query: str) -> tuple[str | None, float]:
        """Look up a cached response by semantic similarity.

        Returns ``(value, score)`` on a hit and ``(None, score)`` on a miss or on a
        rejected false hit.  Privacy-sensitive queries never touch the cache.
        """
        # 1. Privacy guardrail — never serve sensitive queries from cache.
        if _is_uncacheable(query):
            return None, 0.0

        # 2. Evict expired entries lazily on read, then snapshot for scoring.
        now = time.time()
        with self._lock:
            self._entries = [e for e in self._entries if now - e.created_at <= self.ttl_seconds]
            entries = list(self._entries)

        # 3. Find the best matching entry.
        best_entry: CacheEntry | None = None
        best_score = 0.0
        for entry in entries:
            score = self.similarity(query, entry.key)
            if score > best_score:
                best_score = score
                best_entry = entry

        # 4. Only serve above threshold, and only after the false-hit check.
        if best_entry is not None and best_score >= self.similarity_threshold:
            if _looks_like_false_hit(query, best_entry.key):
                with self._lock:
                    self.false_hit_log.append(
                        {
                            "reason": "date_or_number_mismatch",
                            "query": query,
                            "cached_key": best_entry.key,
                            "score": best_score,
                        }
                    )
                return None, best_score
            return best_entry.value, best_score

        # 5. No match above threshold.
        return None, best_score

    def set(self, query: str, value: str, metadata: dict[str, str] | None = None) -> None:
        """Store a response in cache, refusing privacy-sensitive queries."""
        if _is_uncacheable(query):
            return
        with self._lock:
            self._entries.append(
                CacheEntry(key=query, value=value, created_at=time.time(), metadata=metadata or {})
            )

    @staticmethod
    def similarity(a: str, b: str) -> float:
        """Cosine similarity over word tokens + character 3-grams.

        Jaccard over words is too coarse — it collapses toward 0 or 1 for short
        queries.  Cosine over a bag of words *and* n-grams yields the graded
        middle ground that a similarity threshold can actually be tuned against.
        """
        if a == b:
            return 1.0
        vec_a = Counter(_tokenize(a))
        vec_b = Counter(_tokenize(b))
        if not vec_a or not vec_b:
            return 0.0
        dot = sum(vec_a[token] * vec_b[token] for token in vec_a.keys() & vec_b.keys())
        norm_a = math.sqrt(sum(count * count for count in vec_a.values()))
        norm_b = math.sqrt(sum(count * count for count in vec_b.values()))
        if norm_a == 0.0 or norm_b == 0.0:
            return 0.0
        return dot / (norm_a * norm_b)


# ---------------------------------------------------------------------------
# Redis shared cache
# ---------------------------------------------------------------------------


class SharedRedisCache:
    """Redis-backed shared cache so every gateway instance sees the same entries.

    Data model:
        Key   = "{prefix}{query_hash}"
        Value = Redis Hash with fields "query" (original text) and "response"
        TTL   = Redis EXPIRE, so eviction is handled by Redis rather than by us

    The original query text must be stored alongside the response: the similarity
    scan needs to read back the old question in order to compare against it.
    """

    def __init__(
        self,
        redis_url: str,
        ttl_seconds: int,
        similarity_threshold: float,
        prefix: str = "rl:cache:",
    ):
        import redis as redis_lib

        self.ttl_seconds = ttl_seconds
        self.similarity_threshold = similarity_threshold
        self.prefix = prefix
        self.false_hit_log: list[dict[str, object]] = []
        self._redis: Any = redis_lib.Redis.from_url(redis_url, decode_responses=True)

    def ping(self) -> bool:
        """Check Redis connectivity."""
        try:
            return bool(self._redis.ping())
        except Exception:  # noqa: BLE001 - a health check must never raise
            return False

    def get(self, query: str) -> tuple[str | None, float]:
        """Look up a cached response from Redis (exact hash hit, then similarity scan)."""
        if _is_uncacheable(query):
            return None, 0.0

        # Fast path: deterministic hash of the normalised query.
        exact_key = f"{self.prefix}{self._query_hash(query)}"
        exact = self._redis.hget(exact_key, "response")
        if exact is not None:
            return str(exact), 1.0

        # Slow path: scan the namespace and score every stored question.
        best_query: str | None = None
        best_value: str | None = None
        best_score = 0.0
        for key in self._redis.scan_iter(f"{self.prefix}*"):
            cached_query = self._redis.hget(key, "query")
            if cached_query is None:
                continue
            score = ResponseCache.similarity(query, str(cached_query))
            if score > best_score:
                best_score = score
                best_query = str(cached_query)
                best_value = self._redis.hget(key, "response")

        if (
            best_value is not None
            and best_query is not None
            and best_score >= self.similarity_threshold
        ):
            if _looks_like_false_hit(query, best_query):
                self.false_hit_log.append(
                    {
                        "reason": "date_or_number_mismatch",
                        "query": query,
                        "cached_key": best_query,
                        "score": best_score,
                    }
                )
                return None, best_score
            return str(best_value), best_score

        return None, best_score

    def set(self, query: str, value: str, metadata: dict[str, str] | None = None) -> None:
        """Store a response in Redis with TTL, refusing privacy-sensitive queries."""
        if _is_uncacheable(query):
            return
        key = f"{self.prefix}{self._query_hash(query)}"
        mapping: dict[str, str] = {"query": query, "response": value}
        if metadata and metadata.get("provider"):
            mapping["provider"] = metadata["provider"]
        self._redis.hset(key, mapping=mapping)
        self._redis.expire(key, self.ttl_seconds)

    def flush(self) -> None:
        """Remove all entries with this cache prefix (for testing)."""
        for key in self._redis.scan_iter(f"{self.prefix}*"):
            self._redis.delete(key)

    def close(self) -> None:
        """Close Redis connection."""
        if self._redis is not None:
            self._redis.close()

    @staticmethod
    def _query_hash(query: str) -> str:
        """Deterministic short hash for a query string."""
        return hashlib.md5(query.lower().strip().encode()).hexdigest()[:12]
