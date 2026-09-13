#!/usr/bin/env python3
"""
Reliable Async Etherscan BFS Crawler v17

Key improvements over v16:
- Uses the current Etherscan Free-tier-safe default page size of 1,000.
- Binds persistent state to crawl semantics so incompatible restarts fail closed.
- Requeues interrupted in-progress work before exit, not only on the next launch.
- Caps Retry-After delays and treats permanent HTTP/API failures as fatal.
- Detects any repeated pagination signature, not only immediately repeated pages.
- Rejects partially malformed API pages instead of silently dropping rows.
- Optional per-address transaction cap prevents pathological memory growth.
- Optional frontier cap prevents accidental unbounded graph explosions.
- Stronger SQLite schema/version metadata and path-collision checks.
- Better cancellation/error propagation between workers and the writer.
- Writer keeps NDJSON as the durability authority and reconciles SQLite IDs at startup.
- More complete metrics, including retries, pages, API rows and frontier upgrades.
- Correct URL default (plain URL, not Markdown link syntax).

Required environment variables:
  ETHERSCAN_API_KEY=...
  START_ADDRESS=0x...

Common optional variables:
  ETHERSCAN_CHAIN_ID=1
  CRAWL_DEPTH=2
  CRAWL_WORKERS=8
  CRAWL_CONCURRENT_REQUESTS=8
  ETHERSCAN_RATE_LIMIT_PER_SEC=2.8
  ETHERSCAN_BURST_SIZE=3
  ETHERSCAN_PAGE_SIZE=1000
  OUTPUT_FILE=transactions.ndjson
  STATE_DB=transactions.state.sqlite3
  INCLUDE_INTERNAL_TXS=false
  SKIP_SELF_TRANSFERS=false
  RESET_STATE=false
  FSYNC_WRITES=false

Safety limits (0 = unlimited):
  MAX_PAGES_PER_ADDRESS=0
  MAX_TXS_PER_ADDRESS=0
  MAX_FRONTIER_ADDRESSES=0
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import re
import signal
import sqlite3
import time
from dataclasses import asdict, dataclass, field
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Iterable, Optional

import aiofiles
import aiohttp


SCHEMA_VERSION = "17"
DEFAULT_BASE_URL = "https://api.etherscan.io/v2/api"
RETRYABLE_HTTP = {408, 425, 429, 500, 502, 503, 504}
ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


# =============================================================================
# CONFIGURATION
# =============================================================================


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean, got {raw!r}")


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return default if raw is None or not raw.strip() else int(raw)


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return default if raw is None or not raw.strip() else float(raw)


@dataclass(frozen=True, slots=True)
class Config:
    api_key: str = os.getenv("ETHERSCAN_API_KEY", "").strip()
    base_url: str = os.getenv("ETHERSCAN_BASE_URL", DEFAULT_BASE_URL).strip()
    chain_id: str = os.getenv("ETHERSCAN_CHAIN_ID", "1").strip()
    start_address: str = os.getenv("START_ADDRESS", "").strip()

    depth: int = env_int("CRAWL_DEPTH", 2)
    workers: int = env_int("CRAWL_WORKERS", 8)
    concurrent_requests: int = env_int("CRAWL_CONCURRENT_REQUESTS", 8)
    writer_queue_size: int = env_int("WRITER_QUEUE_SIZE", 2_000)

    rate_limit_per_sec: float = env_float("ETHERSCAN_RATE_LIMIT_PER_SEC", 2.8)
    burst_size: int = env_int("ETHERSCAN_BURST_SIZE", 3)

    request_timeout_sec: float = env_float("ETHERSCAN_REQUEST_TIMEOUT", 30.0)
    connect_timeout_sec: float = env_float("ETHERSCAN_CONNECT_TIMEOUT", 10.0)
    sock_read_timeout_sec: float = env_float("ETHERSCAN_SOCK_READ_TIMEOUT", 30.0)
    max_request_retries: int = env_int("ETHERSCAN_MAX_RETRIES", 8)
    max_address_retries: int = env_int("ADDRESS_MAX_RETRIES", 3)
    retry_base_sec: float = env_float("RETRY_BASE_SEC", 0.8)
    retry_cap_sec: float = env_float("RETRY_CAP_SEC", 30.0)
    max_retry_after_sec: float = env_float("MAX_RETRY_AFTER_SEC", 120.0)

    startblock: int = env_int("ETHERSCAN_STARTBLOCK", 0)
    endblock: int = env_int("ETHERSCAN_ENDBLOCK", 99_999_999)
    # Since 2026-07-01 Etherscan Free tier caps affected account endpoints at 1,000.
    page_size: int = env_int("ETHERSCAN_PAGE_SIZE", 1_000)
    max_pages_per_address: int = env_int("MAX_PAGES_PER_ADDRESS", 0)
    max_txs_per_address: int = env_int("MAX_TXS_PER_ADDRESS", 0)
    max_frontier_addresses: int = env_int("MAX_FRONTIER_ADDRESSES", 0)

    output_file: Path = Path(os.getenv("OUTPUT_FILE", "transactions.ndjson").strip())
    state_db: Path = Path(os.getenv("STATE_DB", "transactions.state.sqlite3").strip())
    fsync_writes: bool = env_bool("FSYNC_WRITES", False)
    sqlite_synchronous: str = os.getenv("SQLITE_SYNCHRONOUS", "NORMAL").strip().upper()

    include_internal_txs: bool = env_bool("INCLUDE_INTERNAL_TXS", False)
    skip_self_transfers: bool = env_bool("SKIP_SELF_TRANSFERS", False)
    reset_state: bool = env_bool("RESET_STATE", False)
    log_every_sec: float = env_float("LOG_EVERY_SEC", 5.0)

    def validate(self) -> None:
        errors: list[str] = []
        if not self.api_key:
            errors.append("ETHERSCAN_API_KEY is missing")
        if not self.start_address:
            errors.append("START_ADDRESS is missing")
        if not self.chain_id.isdigit() or int(self.chain_id) <= 0:
            errors.append("ETHERSCAN_CHAIN_ID must be a positive integer")
        if self.depth < 1:
            errors.append("CRAWL_DEPTH must be >= 1")
        if self.workers < 1 or self.concurrent_requests < 1:
            errors.append("CRAWL_WORKERS and CRAWL_CONCURRENT_REQUESTS must be >= 1")
        if self.rate_limit_per_sec <= 0 or self.burst_size < 1:
            errors.append("rate limit must be > 0 and burst size must be >= 1")
        if self.max_request_retries < 1 or self.max_address_retries < 0:
            errors.append("retry counts are invalid")
        if min(self.request_timeout_sec, self.connect_timeout_sec, self.sock_read_timeout_sec) <= 0:
            errors.append("HTTP timeout values must be positive")
        if self.retry_base_sec <= 0 or self.retry_cap_sec < self.retry_base_sec:
            errors.append("retry delay values are invalid")
        if self.max_retry_after_sec <= 0:
            errors.append("MAX_RETRY_AFTER_SEC must be positive")
        if self.writer_queue_size < 1:
            errors.append("WRITER_QUEUE_SIZE must be >= 1")
        if self.log_every_sec <= 0:
            errors.append("LOG_EVERY_SEC must be positive")
        if not self.base_url.startswith(("https://", "http://")):
            errors.append("ETHERSCAN_BASE_URL must be an HTTP(S) URL")
        if not (1 <= self.page_size <= 10_000):
            errors.append("ETHERSCAN_PAGE_SIZE must be between 1 and 10000")
        if self.startblock < 0 or self.endblock < self.startblock:
            errors.append("invalid ETHERSCAN_STARTBLOCK/ETHERSCAN_ENDBLOCK range")
        if self.max_pages_per_address < 0 or self.max_txs_per_address < 0:
            errors.append("MAX_PAGES_PER_ADDRESS/MAX_TXS_PER_ADDRESS cannot be negative")
        if self.max_frontier_addresses < 0:
            errors.append("MAX_FRONTIER_ADDRESSES cannot be negative")
        if self.sqlite_synchronous not in {"OFF", "NORMAL", "FULL", "EXTRA"}:
            errors.append("SQLITE_SYNCHRONOUS must be OFF, NORMAL, FULL, or EXTRA")

        try:
            output = self.output_file.resolve()
            db = self.state_db.resolve()
            forbidden = {
                db,
                Path(str(db) + "-wal"),
                Path(str(db) + "-shm"),
            }
            if output in forbidden:
                errors.append("OUTPUT_FILE collides with STATE_DB or its WAL/SHM files")
        except OSError:
            pass

        if errors:
            raise RuntimeError("Invalid configuration:\n- " + "\n- ".join(errors))


CFG = Config()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("etherscan-bfs")


# =============================================================================
# MODELS
# =============================================================================


@dataclass(frozen=True, slots=True)
class WorkItem:
    address: str
    depth: int


@dataclass(frozen=True, slots=True)
class Transaction:
    record_id: str
    hash: str
    from_addr: str
    to_addr: str
    value_wei: str
    value_eth: str
    timestamp: int
    block_number: int
    kind: str
    trace_id: str = ""
    is_error: str = ""


@dataclass(slots=True)
class FetchOutcome:
    ok: bool
    transactions: list[dict[str, Any]] = field(default_factory=list)
    error: str = ""


@dataclass(slots=True)
class WriteRequest:
    transactions: list[Transaction]
    done: asyncio.Future[int]


@dataclass(slots=True)
class Stats:
    started_monotonic: float = field(default_factory=time.monotonic)
    requests: int = 0
    request_retries: int = 0
    pages: int = 0
    api_rows: int = 0
    http_errors: int = 0
    soft_errors: int = 0
    addresses_done: int = 0
    addresses_failed: int = 0
    address_retries: int = 0
    frontier_inserted: int = 0
    frontier_upgraded: int = 0
    tx_observed: int = 0
    tx_written: int = 0
    tx_duplicates: int = 0
    malformed_transactions: int = 0


class FatalAPIError(RuntimeError):
    pass


class FrontierLimitError(RuntimeError):
    pass


# =============================================================================
# UTILITIES
# =============================================================================


def normalize_address(address: Any) -> Optional[str]:
    if not isinstance(address, str):
        return None
    value = address.strip()
    if not ADDRESS_RE.fullmatch(value):
        return None
    return value.lower()


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def wei_to_eth_str(value: Any) -> str:
    try:
        wei = Decimal(str(value or "0"))
        if not wei.is_finite():
            return "0"
        return format(wei / Decimal("1000000000000000000"), "f")
    except (InvalidOperation, ValueError, TypeError):
        return "0"


def transaction_record_id(tx: dict[str, Any], kind: str) -> str:
    tx_hash = str(tx.get("hash", "")).strip().lower()
    if kind == "internal":
        trace_id = str(tx.get("traceId", "")).strip()
        if not trace_id:
            # Explorer-compatible APIs occasionally omit traceId. Hashing a broad set of
            # trace fields avoids huge record IDs while keeping fallback identity stable.
            fallback = "\x1f".join(
                str(tx.get(key, ""))
                for key in (
                    "blockNumber", "transactionIndex", "type", "callType", "from", "to",
                    "contractAddress", "value", "gas", "gasUsed", "input", "isError",
                )
            )
            trace_id = "fallback-" + hashlib.sha256(fallback.encode("utf-8")).hexdigest()[:24]
        return f"internal:{tx_hash}:{trace_id}"
    return f"normal:{tx_hash}"


def full_jitter_delay(attempt: int, base: float, cap: float) -> float:
    exponent = min(attempt, 30)
    return random.uniform(0.0, min(cap, base * (2**exponent)))


def parse_retry_after(value: Optional[str], cap: float) -> Optional[float]:
    if not value:
        return None
    delay: Optional[float]
    try:
        delay = max(0.0, float(value))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
            delay = max(0.0, retry_at.timestamp() - time.time())
        except (TypeError, ValueError, OverflowError):
            return None
    return min(cap, delay)


def is_no_transactions(data: dict[str, Any]) -> bool:
    result = data.get("result")
    message = str(data.get("message", "")).lower()
    result_text = str(result).lower()
    return (
        result == []
        or "no transactions found" in message
        or "no transactions found" in result_text
    )


def api_error_text(data: dict[str, Any]) -> str:
    parts = (
        str(data.get("message", "")).strip(),
        str(data.get("result", "")).strip(),
    )
    return " | ".join(part for part in parts if part)


def classify_api_error(data: dict[str, Any]) -> str:
    text = api_error_text(data).lower()
    if any(token in text for token in (
        "rate limit", "max rate limit", "too many requests", "temporarily unavailable",
        "timeout", "server busy", "try again", "query timeout",
    )):
        return "retryable"
    if any(token in text for token in (
        "invalid api key", "missing api key", "unsupported chainid", "unsupported chain id",
        "missing chainid", "deprecated v1", "invalid action", "invalid module",
        "invalid address format", "invalid startblock", "invalid endblock",
    )):
        return "fatal"
    return "other"


def build_params(address: str, action: str, page: int) -> dict[str, str | int]:
    return {
        "chainid": CFG.chain_id,
        "module": "account",
        "action": action,
        "address": address,
        "startblock": CFG.startblock,
        "endblock": CFG.endblock,
        "page": page,
        "offset": CFG.page_size,
        "sort": "asc",
        "apikey": CFG.api_key,
    }


def state_semantics(start_address: str) -> dict[str, Any]:
    # Changing any of these can make previously completed addresses semantically stale.
    return {
        "schema_version": SCHEMA_VERSION,
        "chain_id": CFG.chain_id,
        "start_address": start_address,
        "startblock": CFG.startblock,
        "endblock": CFG.endblock,
        "include_internal_txs": CFG.include_internal_txs,
        "skip_self_transfers": CFG.skip_self_transfers,
    }


# =============================================================================
# RATE LIMITER
# =============================================================================


class TokenBucket:
    def __init__(self, rate: float, capacity: int) -> None:
        self.rate = rate
        self.capacity = float(capacity)
        self.tokens = float(capacity)
        self.updated = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self, stop_event: asyncio.Event) -> bool:
        while not stop_event.is_set():
            async with self.lock:
                now = time.monotonic()
                self.tokens = min(
                    self.capacity,
                    self.tokens + (now - self.updated) * self.rate,
                )
                self.updated = now
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return True
                delay = (1.0 - self.tokens) / self.rate
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
        return False


# =============================================================================
# SQLITE STATE
# =============================================================================


class StateStore:
    def __init__(self, path: Path, stats: Stats) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.stats = stats
        self.conn = sqlite3.connect(path, timeout=30.0, isolation_level=None)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute(f"PRAGMA synchronous={CFG.sqlite_synchronous}")
        self.conn.execute("PRAGMA busy_timeout=30000")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS addresses (
                address TEXT PRIMARY KEY,
                depth INTEGER NOT NULL CHECK(depth >= 1),
                status TEXT NOT NULL CHECK(status IN ('queued','in_progress','done','failed')),
                attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
                last_error TEXT NOT NULL DEFAULT '',
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_addresses_status_depth
                ON addresses(status, depth DESC);

            CREATE TABLE IF NOT EXISTS transaction_ids (
                record_id TEXT PRIMARY KEY,
                created_at INTEGER NOT NULL
            );
            """
        )

    def close(self) -> None:
        self.conn.close()

    def reset(self) -> None:
        self.conn.executescript(
            "DELETE FROM addresses; DELETE FROM transaction_ids; DELETE FROM metadata;"
        )

    def bind_semantics(self, semantics: dict[str, Any]) -> None:
        encoded = {key: json.dumps(value, sort_keys=True) for key, value in semantics.items()}
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            existing = dict(self.conn.execute("SELECT key, value FROM metadata").fetchall())
            if not existing:
                self.conn.executemany(
                    "INSERT INTO metadata(key, value) VALUES (?, ?)",
                    encoded.items(),
                )
            else:
                mismatches: list[str] = []
                for key, expected in encoded.items():
                    actual = existing.get(key)
                    if actual != expected:
                        mismatches.append(
                            f"{key}: state={actual!r}, current={expected!r}"
                        )
                if mismatches:
                    raise RuntimeError(
                        "STATE_DB belongs to incompatible crawl semantics. "
                        "Use the original configuration or RESET_STATE=true.\n- "
                        + "\n- ".join(mismatches)
                    )
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    def recover(self) -> int:
        now = int(time.time())
        cur = self.conn.execute(
            "UPDATE addresses SET status='queued', updated_at=? WHERE status='in_progress'",
            (now,),
        )
        return max(0, cur.rowcount)

    def frontier_count(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) FROM addresses").fetchone()
        return int(row[0]) if row else 0

    def enqueue(self, address: str, depth: int) -> bool:
        """Insert/upgrade an address. Return True iff an in-memory wake-up is needed."""
        now = int(time.time())
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            row = self.conn.execute(
                "SELECT depth, status FROM addresses WHERE address=?", (address,)
            ).fetchone()
            should_queue = False

            if row is None:
                if CFG.max_frontier_addresses:
                    count_row = self.conn.execute("SELECT COUNT(*) FROM addresses").fetchone()
                    count = int(count_row[0]) if count_row else 0
                    if count >= CFG.max_frontier_addresses:
                        raise FrontierLimitError(
                            f"MAX_FRONTIER_ADDRESSES={CFG.max_frontier_addresses} reached"
                        )
                self.conn.execute(
                    """
                    INSERT INTO addresses(address, depth, status, attempts, last_error, updated_at)
                    VALUES (?, ?, 'queued', 0, '', ?)
                    """,
                    (address, depth, now),
                )
                self.stats.frontier_inserted += 1
                should_queue = True
            else:
                old_depth, status = int(row[0]), str(row[1])
                if depth > old_depth:
                    new_status = "in_progress" if status == "in_progress" else "queued"
                    self.conn.execute(
                        """
                        UPDATE addresses
                        SET depth=?, status=?, attempts=0, last_error='', updated_at=?
                        WHERE address=?
                        """,
                        (depth, new_status, now, address),
                    )
                    self.stats.frontier_upgraded += 1
                    # queued already has a queue token; in_progress will be requeued by complete().
                    should_queue = status in {"done", "failed"}

            self.conn.execute("COMMIT")
            return should_queue
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    def queued_items(self) -> list[WorkItem]:
        rows = self.conn.execute(
            "SELECT address, depth FROM addresses WHERE status='queued' ORDER BY depth DESC, address"
        ).fetchall()
        return [WorkItem(address=str(row[0]), depth=int(row[1])) for row in rows]

    def claim(self, address: str) -> Optional[WorkItem]:
        now = int(time.time())
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            row = self.conn.execute(
                "SELECT depth, status FROM addresses WHERE address=?", (address,)
            ).fetchone()
            if row is None or str(row[1]) != "queued":
                self.conn.execute("COMMIT")
                return None

            depth = int(row[0])
            cur = self.conn.execute(
                """
                UPDATE addresses
                SET status='in_progress', updated_at=?
                WHERE address=? AND status='queued'
                """,
                (now, address),
            )
            self.conn.execute("COMMIT")
            if cur.rowcount != 1:
                return None
            return WorkItem(address=address, depth=depth)
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    def complete(self, item: WorkItem) -> Optional[WorkItem]:
        now = int(time.time())
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            row = self.conn.execute(
                "SELECT depth, status FROM addresses WHERE address=?", (item.address,)
            ).fetchone()
            if row is None:
                self.conn.execute("COMMIT")
                return None

            durable_depth, status = int(row[0]), str(row[1])
            if status != "in_progress":
                self.conn.execute("COMMIT")
                return None

            if durable_depth > item.depth:
                self.conn.execute(
                    """
                    UPDATE addresses
                    SET status='queued', attempts=0, last_error='', updated_at=?
                    WHERE address=?
                    """,
                    (now, item.address),
                )
                self.conn.execute("COMMIT")
                return WorkItem(item.address, durable_depth)

            self.conn.execute(
                """
                UPDATE addresses
                SET status='done', attempts=0, last_error='', updated_at=?
                WHERE address=?
                """,
                (now, item.address),
            )
            self.conn.execute("COMMIT")
            return None
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    def mark_retry_or_failed(self, item: WorkItem, error: str) -> Optional[WorkItem]:
        now = int(time.time())
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            row = self.conn.execute(
                "SELECT depth, attempts, status FROM addresses WHERE address=?",
                (item.address,),
            ).fetchone()
            if row is None:
                self.conn.execute("COMMIT")
                return None

            durable_depth, old_attempts, status = int(row[0]), int(row[1]), str(row[2])
            # Do not overwrite a state another path has already moved away from in_progress.
            if status != "in_progress":
                self.conn.execute("COMMIT")
                return None

            attempts = old_attempts + 1
            retry = attempts <= CFG.max_address_retries
            self.conn.execute(
                """
                UPDATE addresses
                SET status=?, attempts=?, last_error=?, updated_at=?
                WHERE address=?
                """,
                (
                    "queued" if retry else "failed",
                    attempts,
                    error[:1000],
                    now,
                    item.address,
                ),
            )
            self.conn.execute("COMMIT")
            return WorkItem(item.address, durable_depth) if retry else None
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    def insert_transaction_ids(self, ids: Iterable[str]) -> int:
        now = int(time.time())
        inserted = 0
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            for record_id in ids:
                cur = self.conn.execute(
                    "INSERT OR IGNORE INTO transaction_ids(record_id, created_at) VALUES (?, ?)",
                    (record_id, now),
                )
                if cur.rowcount == 1:
                    inserted += 1
            self.conn.execute("COMMIT")
            return inserted
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    def counts(self) -> dict[str, int]:
        result = {"queued": 0, "in_progress": 0, "done": 0, "failed": 0}
        for status, count in self.conn.execute(
            "SELECT status, COUNT(*) FROM addresses GROUP BY status"
        ):
            result[str(status)] = int(count)
        return result


# =============================================================================
# HTTP CLIENT
# =============================================================================


class EtherscanClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        limiter: TokenBucket,
        stop_event: asyncio.Event,
        stats: Stats,
    ) -> None:
        self.session = session
        self.semaphore = semaphore
        self.limiter = limiter
        self.stop_event = stop_event
        self.stats = stats

    async def _sleep_or_stop(self, delay: float) -> bool:
        if delay <= 0:
            return not self.stop_event.is_set()
        try:
            await asyncio.wait_for(self.stop_event.wait(), timeout=delay)
            return False
        except asyncio.TimeoutError:
            return True

    async def _request(self, params: dict[str, Any]) -> Optional[dict[str, Any]]:
        last_error = "request failed"
        for attempt in range(CFG.max_request_retries):
            if self.stop_event.is_set():
                return None
            if not await self.limiter.acquire(self.stop_event):
                return None

            if attempt:
                self.stats.request_retries += 1

            try:
                async with self.semaphore:
                    async with self.session.get(CFG.base_url, params=params) as response:
                        self.stats.requests += 1
                        status = response.status
                        body = await response.text()

                        if status in RETRYABLE_HTTP:
                            self.stats.http_errors += 1
                            last_error = f"HTTP {status}"
                            if attempt + 1 >= CFG.max_request_retries:
                                break
                            delay = parse_retry_after(
                                response.headers.get("Retry-After"), CFG.max_retry_after_sec
                            )
                            if delay is None:
                                delay = full_jitter_delay(
                                    attempt, CFG.retry_base_sec, CFG.retry_cap_sec
                                )
                            if not await self._sleep_or_stop(delay):
                                return None
                            continue

                        if status in {401, 403}:
                            raise FatalAPIError(f"HTTP {status}: {body[:500]}")

                        if status != 200:
                            self.stats.http_errors += 1
                            last_error = f"HTTP {status}: {body[:300]}"
                            # Other 4xx responses are permanent for the exact request.
                            if 400 <= status < 500:
                                raise FatalAPIError(last_error)
                            if attempt + 1 >= CFG.max_request_retries:
                                break
                            if not await self._sleep_or_stop(
                                full_jitter_delay(attempt, CFG.retry_base_sec, CFG.retry_cap_sec)
                            ):
                                return None
                            continue

                        try:
                            data = json.loads(body)
                        except json.JSONDecodeError as exc:
                            self.stats.http_errors += 1
                            last_error = f"invalid JSON: {exc}"
                            if attempt + 1 >= CFG.max_request_retries:
                                break
                            if not await self._sleep_or_stop(
                                full_jitter_delay(attempt, CFG.retry_base_sec, CFG.retry_cap_sec)
                            ):
                                return None
                            continue

                        if not isinstance(data, dict):
                            self.stats.http_errors += 1
                            last_error = f"unexpected JSON root type: {type(data).__name__}"
                            if attempt + 1 >= CFG.max_request_retries:
                                break
                            if not await self._sleep_or_stop(
                                full_jitter_delay(attempt, CFG.retry_base_sec, CFG.retry_cap_sec)
                            ):
                                return None
                            continue

                        status_value = str(data.get("status", ""))
                        if status_value == "0" and not is_no_transactions(data):
                            classification = classify_api_error(data)
                            text = api_error_text(data) or "unknown Etherscan API error"
                            if classification == "fatal":
                                raise FatalAPIError(text)
                            if classification == "retryable":
                                self.stats.soft_errors += 1
                                last_error = text
                                if attempt + 1 >= CFG.max_request_retries:
                                    break
                                if not await self._sleep_or_stop(
                                    full_jitter_delay(
                                        attempt, CFG.retry_base_sec, CFG.retry_cap_sec
                                    )
                                ):
                                    return None
                                continue
                            # Unknown NOTOK is not safe to reinterpret as a valid empty result.
                            raise FatalAPIError(f"Non-retryable Etherscan API error: {text}")

                        return data

            except FatalAPIError:
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                self.stats.http_errors += 1
                last_error = repr(exc)
                if attempt + 1 >= CFG.max_request_retries:
                    break
                if not await self._sleep_or_stop(
                    full_jitter_delay(attempt, CFG.retry_base_sec, CFG.retry_cap_sec)
                ):
                    return None

        logger.debug(
            "Request exhausted retries action=%s address=%s: %s",
            params.get("action"), params.get("address"), last_error,
        )
        return None

    async def fetch_action(self, address: str, action: str) -> FetchOutcome:
        result: list[dict[str, Any]] = []
        page = 1
        seen_signatures: set[tuple[str, str, int]] = set()

        def row_identity(row: dict[str, Any]) -> str:
            return "|".join(
                str(row.get(key, ""))
                for key in (
                    "hash", "traceId", "blockNumber", "transactionIndex", "from", "to",
                    "contractAddress", "value", "type", "callType",
                )
            )

        while not self.stop_event.is_set():
            if CFG.max_pages_per_address and page > CFG.max_pages_per_address:
                return FetchOutcome(
                    ok=False,
                    error=f"MAX_PAGES_PER_ADDRESS exceeded for {action}",
                )

            data = await self._request(build_params(address, action, page))
            if data is None:
                return FetchOutcome(ok=False, error=f"request failed for {action} page {page}")
            self.stats.pages += 1

            if is_no_transactions(data):
                return FetchOutcome(ok=True, transactions=result)

            page_rows = data.get("result")
            if not isinstance(page_rows, list):
                return FetchOutcome(
                    ok=False,
                    error=f"unexpected result for {action}: {api_error_text(data)[:300]}",
                )
            if any(not isinstance(row, dict) for row in page_rows):
                return FetchOutcome(
                    ok=False,
                    error=f"malformed row(s) for {action} page {page}",
                )

            valid_rows: list[dict[str, Any]] = page_rows
            self.stats.api_rows += len(valid_rows)

            if valid_rows:
                signature = (
                    row_identity(valid_rows[0]),
                    row_identity(valid_rows[-1]),
                    len(valid_rows),
                )
                if signature in seen_signatures:
                    return FetchOutcome(
                        ok=False,
                        error=f"pagination cycle detected for {action} page {page}",
                    )
                seen_signatures.add(signature)

            if CFG.max_txs_per_address and len(result) + len(valid_rows) > CFG.max_txs_per_address:
                return FetchOutcome(
                    ok=False,
                    error=(
                        f"MAX_TXS_PER_ADDRESS={CFG.max_txs_per_address} exceeded "
                        f"for {action}"
                    ),
                )

            result.extend(valid_rows)
            if len(page_rows) < CFG.page_size:
                return FetchOutcome(ok=True, transactions=result)
            page += 1

        return FetchOutcome(ok=False, error="shutdown")

    async def fetch_transactions(self, address: str) -> FetchOutcome:
        normal = await self.fetch_action(address, "txlist")
        if not normal.ok:
            return normal
        for tx in normal.transactions:
            tx["_kind"] = "normal"

        if not CFG.include_internal_txs:
            return normal

        internal = await self.fetch_action(address, "txlistinternal")
        if not internal.ok:
            return internal
        for tx in internal.transactions:
            tx["_kind"] = "internal"
        return FetchOutcome(ok=True, transactions=normal.transactions + internal.transactions)


# =============================================================================
# WRITER
# =============================================================================


async def load_output_ids(path: Path) -> set[str]:
    ids: set[str] = set()
    if not path.exists():
        return ids

    logger.info("Scanning existing output for restart dedupe: %s", path)
    line_no = 0
    async with aiofiles.open(path, "r", encoding="utf-8") as file:
        async for line in file:
            line_no += 1
            if not line.strip():
                continue
            try:
                row = json.loads(line)
                if not isinstance(row, dict):
                    continue
                record_id = row.get("record_id")
                if record_id:
                    ids.add(str(record_id))
                elif row.get("hash"):
                    # Compatibility with old normal-transaction-only output.
                    ids.add(f"normal:{str(row['hash']).lower()}")
            except (json.JSONDecodeError, AttributeError, TypeError):
                logger.warning("Ignoring malformed existing NDJSON line %s", line_no)
    logger.info("Loaded %s output record IDs", len(ids))
    return ids


async def writer_loop(
    store: StateStore,
    queue: asyncio.Queue[Optional[WriteRequest]],
    output_ids: set[str],
    stats: Stats,
) -> None:
    CFG.output_file.parent.mkdir(parents=True, exist_ok=True)
    async with aiofiles.open(CFG.output_file, "a", encoding="utf-8") as file:
        while True:
            request = await queue.get()
            try:
                if request is None:
                    return

                unique: dict[str, Transaction] = {}
                for tx in request.transactions:
                    unique.setdefault(tx.record_id, tx)

                to_append = [
                    tx for tx in unique.values() if tx.record_id not in output_ids
                ]

                # NDJSON is authoritative. Persist it first. Only after the append is
                # flushed do we mirror IDs into SQLite. If the process dies in-between,
                # startup rescans NDJSON and repairs SQLite without duplicating output.
                if to_append:
                    payload = "".join(
                        json.dumps(asdict(tx), ensure_ascii=False, separators=(",", ":")) + "\n"
                        for tx in to_append
                    )
                    await file.write(payload)
                    await file.flush()
                    if CFG.fsync_writes:
                        await asyncio.to_thread(os.fsync, file.fileno())
                    output_ids.update(tx.record_id for tx in to_append)

                store.insert_transaction_ids(unique.keys())
                appended = len(to_append)
                stats.tx_written += appended
                stats.tx_duplicates += len(request.transactions) - appended
                if not request.done.done():
                    request.done.set_result(appended)
            except asyncio.CancelledError:
                if request is not None and not request.done.done():
                    request.done.cancel()
                raise
            except Exception as exc:
                if request is not None and not request.done.done():
                    request.done.set_exception(exc)
                raise
            finally:
                queue.task_done()


# =============================================================================
# CRAWL WORKERS
# =============================================================================


def parse_transactions(
    rows: list[dict[str, Any]], stats: Stats
) -> tuple[list[Transaction], set[str]]:
    records: list[Transaction] = []
    neighbors: set[str] = set()

    for tx in rows:
        tx_hash = str(tx.get("hash", "")).strip().lower()
        if not tx_hash:
            stats.malformed_transactions += 1
            continue

        from_addr = normalize_address(tx.get("from"))
        raw_to = tx.get("to")
        to_addr = normalize_address(raw_to)
        contract_address = normalize_address(tx.get("contractAddress"))

        if not from_addr:
            stats.malformed_transactions += 1
            continue

        # Empty `to` is legitimate for contract creation.
        if raw_to not in (None, "") and not to_addr:
            stats.malformed_transactions += 1
            continue

        output_to = to_addr or contract_address or ""
        if CFG.skip_self_transfers and to_addr and from_addr == to_addr:
            continue

        kind = str(tx.get("_kind", "normal"))
        value_wei = str(tx.get("value", "0") or "0")
        records.append(
            Transaction(
                record_id=transaction_record_id(tx, kind),
                hash=tx_hash,
                from_addr=from_addr,
                to_addr=output_to,
                value_wei=value_wei,
                value_eth=wei_to_eth_str(value_wei),
                timestamp=safe_int(tx.get("timeStamp")),
                block_number=safe_int(tx.get("blockNumber")),
                kind=kind,
                trace_id=str(tx.get("traceId", "")),
                is_error=str(tx.get("isError", "")),
            )
        )

        neighbors.add(from_addr)
        if to_addr:
            neighbors.add(to_addr)
        if contract_address:
            neighbors.add(contract_address)

    stats.tx_observed += len(records)
    return records, neighbors


async def enqueue_retry_with_backoff(
    work_queue: asyncio.Queue[WorkItem],
    item: WorkItem,
    attempts: int,
    stop_event: asyncio.Event,
) -> None:
    delay = full_jitter_delay(
        max(0, attempts - 1), CFG.retry_base_sec, CFG.retry_cap_sec
    )
    if delay > 0:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=delay)
            return
        except asyncio.TimeoutError:
            pass
    if not stop_event.is_set():
        await work_queue.put(item)


async def worker_loop(
    worker_id: int,
    client: EtherscanClient,
    store: StateStore,
    work_queue: asyncio.Queue[WorkItem],
    writer_queue: asyncio.Queue[Optional[WriteRequest]],
    stop_event: asyncio.Event,
    fatal_event: asyncio.Event,
    stats: Stats,
) -> None:
    while True:
        item = await work_queue.get()
        claimed: Optional[WorkItem] = None
        try:
            if stop_event.is_set():
                continue

            claimed = store.claim(item.address)
            if claimed is None:
                continue
            item = claimed

            outcome = await client.fetch_transactions(item.address)
            if not outcome.ok:
                retry_item = store.mark_retry_or_failed(item, outcome.error)
                if retry_item is not None and not stop_event.is_set():
                    stats.address_retries += 1
                    # Address-level backoff avoids a hot retry loop after HTTP retries fail.
                    attempts_row = store.conn.execute(
                        "SELECT attempts FROM addresses WHERE address=?", (item.address,)
                    ).fetchone()
                    attempts = int(attempts_row[0]) if attempts_row else 1
                    await enqueue_retry_with_backoff(
                        work_queue, retry_item, attempts, stop_event
                    )
                else:
                    stats.addresses_failed += 1
                continue

            records, neighbors = parse_transactions(outcome.transactions, stats)
            if records:
                future: asyncio.Future[int] = asyncio.get_running_loop().create_future()
                await writer_queue.put(WriteRequest(records, future))
                await future

            if item.depth > 1:
                next_depth = item.depth - 1
                for neighbor in neighbors:
                    if neighbor == item.address:
                        continue
                    if store.enqueue(neighbor, next_depth):
                        await work_queue.put(WorkItem(neighbor, next_depth))

            requeue_item = store.complete(item)
            if requeue_item is not None:
                if not stop_event.is_set():
                    await work_queue.put(requeue_item)
            else:
                stats.addresses_done += 1

        except FatalAPIError as exc:
            logger.error("Fatal Etherscan API error: %s", exc)
            if claimed is not None:
                store.mark_retry_or_failed(item, str(exc))
            fatal_event.set()
            stop_event.set()
        except FrontierLimitError as exc:
            logger.error("Frontier safety limit reached: %s", exc)
            if claimed is not None:
                store.mark_retry_or_failed(item, str(exc))
            fatal_event.set()
            stop_event.set()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Worker %s failed for %s", worker_id, item.address)
            if claimed is not None:
                retry_item = store.mark_retry_or_failed(item, repr(exc))
                if retry_item is not None and not stop_event.is_set():
                    stats.address_retries += 1
                    await work_queue.put(retry_item)
                else:
                    stats.addresses_failed += 1
            else:
                fatal_event.set()
                stop_event.set()
        finally:
            work_queue.task_done()


async def monitor_loop(
    store: StateStore,
    work_queue: asyncio.Queue[WorkItem],
    writer_queue: asyncio.Queue[Optional[WriteRequest]],
    stop_event: asyncio.Event,
    stats: Stats,
) -> None:
    last_requests = 0
    last_rows = 0
    last_time = stats.started_monotonic

    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=CFG.log_every_sec)
            return
        except asyncio.TimeoutError:
            pass

        counts = store.counts()
        now = time.monotonic()
        elapsed = max(0.001, now - stats.started_monotonic)
        interval = max(0.001, now - last_time)
        req_rate = (stats.requests - last_requests) / interval
        row_rate = (stats.api_rows - last_rows) / interval
        last_requests = stats.requests
        last_rows = stats.api_rows
        last_time = now

        logger.info(
            "addr done=%s queued=%s active=%s failed=%s | work_q=%s writer_q=%s | "
            "tx observed=%s new=%s dup=%s malformed=%s | req=%s retry=%s pages=%s "
            "http_err=%s api_retry=%s | %.2f req/s %.1f rows/s | elapsed=%.1fs",
            counts["done"], counts["queued"], counts["in_progress"], counts["failed"],
            work_queue.qsize(), writer_queue.qsize(), stats.tx_observed,
            stats.tx_written, stats.tx_duplicates, stats.malformed_transactions,
            stats.requests, stats.request_retries, stats.pages, stats.http_errors,
            stats.soft_errors, req_rate, row_rate, elapsed,
        )


# =============================================================================
# ORCHESTRATION
# =============================================================================


def remove_state_files(path: Path) -> None:
    for candidate in (path, Path(str(path) + "-wal"), Path(str(path) + "-shm")):
        candidate.unlink(missing_ok=True)


async def run() -> int:
    CFG.validate()
    start = normalize_address(CFG.start_address)
    if not start:
        raise RuntimeError("START_ADDRESS is not a valid EVM address")

    if CFG.page_size > 1_000:
        logger.warning(
            "ETHERSCAN_PAGE_SIZE=%s exceeds the current Free-tier cap of 1000 for "
            "affected account endpoints; use this only if your plan supports it",
            CFG.page_size,
        )

    if CFG.reset_state:
        CFG.output_file.unlink(missing_ok=True)
        remove_state_files(CFG.state_db)

    stats = Stats()
    store = StateStore(CFG.state_db, stats)
    stop_event = asyncio.Event()
    fatal_event = asyncio.Event()

    loop = asyncio.get_running_loop()

    def request_shutdown() -> None:
        if not stop_event.is_set():
            logger.warning("Shutdown requested; unfinished work will remain restartable")
            stop_event.set()

    installed_signals: list[signal.Signals] = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, request_shutdown)
            installed_signals.append(sig)
        except (NotImplementedError, RuntimeError):
            pass

    try:
        store.bind_semantics(state_semantics(start))
        recovered = store.recover()
        if recovered:
            logger.warning("Recovered %s interrupted in-progress address(es)", recovered)

        store.enqueue(start, CFG.depth)
        queued = store.queued_items()
        if not queued:
            counts = store.counts()
            logger.info(
                "No queued addresses. Existing state: done=%s failed=%s",
                counts["done"], counts["failed"],
            )
            return 0

        work_queue: asyncio.Queue[WorkItem] = asyncio.Queue()
        writer_queue: asyncio.Queue[Optional[WriteRequest]] = asyncio.Queue(
            CFG.writer_queue_size
        )
        for item in queued:
            work_queue.put_nowait(item)

        output_ids = await load_output_ids(CFG.output_file)
        repaired = store.insert_transaction_ids(output_ids)
        if repaired:
            logger.info("Reconciled %s transaction IDs into SQLite", repaired)

        timeout = aiohttp.ClientTimeout(
            total=CFG.request_timeout_sec,
            connect=CFG.connect_timeout_sec,
            sock_read=CFG.sock_read_timeout_sec,
        )
        connector = aiohttp.TCPConnector(
            limit=CFG.concurrent_requests,
            limit_per_host=CFG.concurrent_requests,
            ttl_dns_cache=300,
            keepalive_timeout=60,
            enable_cleanup_closed=True,
        )
        headers = {
            "Accept": "application/json",
            "User-Agent": "etherscan-bfs-crawler/17.0",
        }
        limiter = TokenBucket(CFG.rate_limit_per_sec, CFG.burst_size)
        semaphore = asyncio.Semaphore(CFG.concurrent_requests)

        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers=headers,
            raise_for_status=False,
        ) as session:
            client = EtherscanClient(session, semaphore, limiter, stop_event, stats)
            writer_task = asyncio.create_task(
                writer_loop(store, writer_queue, output_ids, stats), name="writer"
            )
            workers = [
                asyncio.create_task(
                    worker_loop(
                        i, client, store, work_queue, writer_queue,
                        stop_event, fatal_event, stats,
                    ),
                    name=f"worker-{i}",
                )
                for i in range(CFG.workers)
            ]
            monitor_task = asyncio.create_task(
                monitor_loop(store, work_queue, writer_queue, stop_event, stats),
                name="monitor",
            )

            join_task = asyncio.create_task(work_queue.join(), name="work-join")
            stop_task = asyncio.create_task(stop_event.wait(), name="stop-wait")

            done, _ = await asyncio.wait(
                {join_task, stop_task, writer_task},
                return_when=asyncio.FIRST_COMPLETED,
            )

            normal_drain = join_task in done and not stop_event.is_set()

            if writer_task in done:
                try:
                    exc = writer_task.exception()
                except asyncio.CancelledError:
                    exc = RuntimeError("writer task was unexpectedly cancelled")
                if exc is not None:
                    logger.error("Writer task failed: %r", exc)
                else:
                    logger.error("Writer task exited unexpectedly")
                fatal_event.set()
                stop_event.set()
            elif normal_drain:
                stop_event.set()

            # Stop workers. Any address cancelled while in_progress is requeued below.
            for worker in workers:
                worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

            # Requests already put into the writer queue must finish before writer shutdown.
            if not writer_task.done():
                await writer_queue.join()
                await writer_queue.put(None)
                await writer_task
            else:
                await asyncio.gather(writer_task, return_exceptions=True)

            monitor_task.cancel()
            await asyncio.gather(monitor_task, return_exceptions=True)

            for task in (join_task, stop_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(join_task, stop_task, return_exceptions=True)

        # Make the DB immediately restart-clean even after Ctrl+C/fatal cancellation.
        requeued = store.recover()
        if requeued:
            logger.info("Requeued %s interrupted address(es) before exit", requeued)

        counts = store.counts()
        elapsed = max(0.001, time.monotonic() - stats.started_monotonic)
        logger.info(
            "Done | complete=%s queued=%s active=%s failed=%s | tx_new=%s dup=%s "
            "malformed=%s | requests=%s retries=%s pages=%s | elapsed=%.1fs",
            counts["done"], counts["queued"], counts["in_progress"], counts["failed"],
            stats.tx_written, stats.tx_duplicates, stats.malformed_transactions,
            stats.requests, stats.request_retries, stats.pages, elapsed,
        )

        if fatal_event.is_set():
            return 2
        if counts["failed"]:
            # Crawl completed as far as possible but contains permanently failed addresses.
            return 3
        return 0
    finally:
        for sig in installed_signals:
            try:
                loop.remove_signal_handler(sig)
            except (NotImplementedError, RuntimeError):
                pass
        store.close()


def main() -> int:
    try:
        return asyncio.run(run())
    except KeyboardInterrupt:
        logger.warning("Interrupted")
        return 130
    except Exception:
        logger.exception("Fatal error")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
