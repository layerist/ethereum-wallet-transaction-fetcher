#!/usr/bin/env python3
"""
Reliable Async Etherscan BFS Crawler v14

Main changes from v13:
- Uses Etherscan API V2 by default (chainid is required).
- Durable SQLite frontier: a restart continues queued/in-progress addresses.
- A dedicated bounded writer queue provides backpressure and owns all NDJSON writes.
- Address is marked done only after its transactions are durably persisted.
- Internal transactions are deduplicated by hash + traceId, not only by hash.
- Fetch failures are distinguishable from legitimate empty transaction lists.
- Per-address retries prevent transient failures from silently marking an address visited.
- Retry-After support, exponential backoff with full jitter, and fatal API error detection.
- Atomic state transitions and graceful Ctrl+C/SIGTERM shutdown.
- Optional fsync for stronger durability.

Required environment variables:
  ETHERSCAN_API_KEY=...
  START_ADDRESS=0x...

Common optional variables:
  ETHERSCAN_CHAIN_ID=1
  CRAWL_DEPTH=2
  CRAWL_WORKERS=8
  CRAWL_CONCURRENT_REQUESTS=8
  ETHERSCAN_RATE_LIMIT_PER_SEC=2.8   # Free tier is currently 3 calls/sec
  ETHERSCAN_BURST_SIZE=3
  OUTPUT_FILE=transactions.ndjson
  STATE_DB=transactions.state.sqlite3
  INCLUDE_INTERNAL_TXS=false
  SKIP_SELF_TRANSFERS=false
  RESET_STATE=false
  FSYNC_WRITES=false
"""

from __future__ import annotations

import asyncio
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
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import urlencode

import aiofiles
import aiohttp


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
    base_url: str = os.getenv(
        "ETHERSCAN_BASE_URL", "https://api.etherscan.io/v2/api"
    ).strip()
    chain_id: str = os.getenv("ETHERSCAN_CHAIN_ID", "1").strip()
    start_address: str = os.getenv("START_ADDRESS", "").strip()

    depth: int = env_int("CRAWL_DEPTH", 2)
    workers: int = env_int("CRAWL_WORKERS", 8)
    concurrent_requests: int = env_int("CRAWL_CONCURRENT_REQUESTS", 8)
    work_queue_size: int = env_int("CRAWL_WORK_QUEUE_SIZE", 20_000)
    writer_queue_size: int = env_int("WRITER_QUEUE_SIZE", 2_000)

    rate_limit_per_sec: float = env_float("ETHERSCAN_RATE_LIMIT_PER_SEC", 2.8)
    burst_size: int = env_int("ETHERSCAN_BURST_SIZE", 3)

    request_timeout_sec: float = env_float("ETHERSCAN_REQUEST_TIMEOUT", 30.0)
    connect_timeout_sec: float = env_float("ETHERSCAN_CONNECT_TIMEOUT", 10.0)
    max_request_retries: int = env_int("ETHERSCAN_MAX_RETRIES", 8)
    max_address_retries: int = env_int("ADDRESS_MAX_RETRIES", 3)
    retry_base_sec: float = env_float("RETRY_BASE_SEC", 0.8)
    retry_cap_sec: float = env_float("RETRY_CAP_SEC", 30.0)

    startblock: int = env_int("ETHERSCAN_STARTBLOCK", 0)
    endblock: int = env_int("ETHERSCAN_ENDBLOCK", 99_999_999)
    page_size: int = env_int("ETHERSCAN_PAGE_SIZE", 10_000)
    max_pages_per_address: int = env_int("MAX_PAGES_PER_ADDRESS", 0)

    output_file: Path = Path(os.getenv("OUTPUT_FILE", "transactions.ndjson").strip())
    state_db: Path = Path(
        os.getenv("STATE_DB", "transactions.state.sqlite3").strip()
    )
    writer_batch_size: int = env_int("WRITER_BATCH_SIZE", 1_000)
    writer_flush_sec: float = env_float("WRITER_FLUSH_SEC", 2.0)
    fsync_writes: bool = env_bool("FSYNC_WRITES", False)

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
            errors.append("worker/concurrency values must be >= 1")
        if self.rate_limit_per_sec <= 0 or self.burst_size < 1:
            errors.append("rate limit and burst size must be positive")
        if self.max_request_retries < 1 or self.max_address_retries < 0:
            errors.append("retry values are invalid")
        if not (1 <= self.page_size <= 10_000):
            errors.append("ETHERSCAN_PAGE_SIZE must be between 1 and 10000")
        if self.startblock < 0 or self.endblock < self.startblock:
            errors.append("invalid startblock/endblock range")
        if self.writer_batch_size < 1:
            errors.append("WRITER_BATCH_SIZE must be >= 1")
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
    http_errors: int = 0
    soft_errors: int = 0
    addresses_done: int = 0
    addresses_failed: int = 0
    tx_observed: int = 0
    tx_written: int = 0
    tx_duplicates: int = 0


class FatalAPIError(RuntimeError):
    pass


# =============================================================================
# UTILITIES
# =============================================================================


ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


def normalize_address(address: Any) -> Optional[str]:
    """Validate and canonicalize an EVM address without requiring web3.py."""
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
    tx_hash = str(tx.get("hash", "")).lower()
    if kind == "internal":
        trace_id = str(tx.get("traceId", ""))
        # Some explorer-compatible APIs omit traceId. The fallback keeps traces distinct.
        if not trace_id:
            trace_id = ":".join(
                str(tx.get(key, ""))
                for key in ("blockNumber", "transactionIndex", "from", "to", "value")
            )
        return f"internal:{tx_hash}:{trace_id}"
    return f"normal:{tx_hash}"


def full_jitter_delay(attempt: int, base: float, cap: float) -> float:
    return random.uniform(0.0, min(cap, base * (2**attempt)))


def parse_retry_after(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None


def is_no_transactions(data: dict[str, Any]) -> bool:
    result = data.get("result")
    message = str(data.get("message", "")).lower()
    return result == [] or "no transactions found" in message


def api_error_text(data: dict[str, Any]) -> str:
    return " | ".join(
        part for part in (
            str(data.get("message", "")).strip(),
            str(data.get("result", "")).strip(),
        ) if part
    )


def classify_api_error(data: dict[str, Any]) -> str:
    text = api_error_text(data).lower()
    if any(token in text for token in (
        "rate limit", "too many requests", "temporarily unavailable", "timeout"
    )):
        return "retryable"
    if any(token in text for token in (
        "invalid api key", "missing or unsupported chainid", "deprecated v1",
        "invalid action", "invalid module"
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
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path, timeout=30.0, isolation_level=None)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA busy_timeout=30000")
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS addresses (
                address TEXT PRIMARY KEY,
                depth INTEGER NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('queued','in_progress','done','failed')),
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT '',
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_addresses_status
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
        self.conn.executescript("DELETE FROM addresses; DELETE FROM transaction_ids;")

    def recover(self) -> None:
        now = int(time.time())
        self.conn.execute(
            "UPDATE addresses SET status='queued', updated_at=? WHERE status='in_progress'",
            (now,),
        )

    def enqueue(self, address: str, depth: int) -> bool:
        cur = self.conn.execute(
            """
            INSERT OR IGNORE INTO addresses(address, depth, status, attempts, updated_at)
            VALUES (?, ?, 'queued', 0, ?)
            """,
            (address, depth, int(time.time())),
        )
        return cur.rowcount == 1

    def queued_items(self) -> list[WorkItem]:
        rows = self.conn.execute(
            "SELECT address, depth FROM addresses WHERE status='queued' ORDER BY depth DESC"
        ).fetchall()
        return [WorkItem(address=row[0], depth=row[1]) for row in rows]

    def mark_in_progress(self, address: str) -> bool:
        cur = self.conn.execute(
            """
            UPDATE addresses SET status='in_progress', updated_at=?
            WHERE address=? AND status='queued'
            """,
            (int(time.time()), address),
        )
        return cur.rowcount == 1

    def mark_done(self, address: str) -> None:
        self.conn.execute(
            "UPDATE addresses SET status='done', last_error='', updated_at=? WHERE address=?",
            (int(time.time()), address),
        )

    def mark_retry_or_failed(self, item: WorkItem, error: str) -> bool:
        row = self.conn.execute(
            "SELECT attempts FROM addresses WHERE address=?", (item.address,)
        ).fetchone()
        attempts = (row[0] if row else 0) + 1
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
                int(time.time()),
                item.address,
            ),
        )
        return retry

    def insert_transaction_ids(self, ids: Iterable[str]) -> set[str]:
        new_ids: set[str] = set()
        now = int(time.time())
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            for record_id in ids:
                cur = self.conn.execute(
                    "INSERT OR IGNORE INTO transaction_ids(record_id, created_at) VALUES (?, ?)",
                    (record_id, now),
                )
                if cur.rowcount == 1:
                    new_ids.add(record_id)
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise
        return new_ids

    def counts(self) -> dict[str, int]:
        result = {"queued": 0, "in_progress": 0, "done": 0, "failed": 0}
        for status, count in self.conn.execute(
            "SELECT status, COUNT(*) FROM addresses GROUP BY status"
        ):
            result[status] = count
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

    async def _request(self, params: dict[str, Any]) -> Optional[dict[str, Any]]:
        for attempt in range(CFG.max_request_retries):
            if not await self.limiter.acquire(self.stop_event):
                return None
            try:
                async with self.semaphore:
                    async with self.session.get(CFG.base_url, params=params) as response:
                        self.stats.requests += 1
                        if response.status == 429:
                            self.stats.http_errors += 1
                            delay = parse_retry_after(response.headers.get("Retry-After"))
                            if delay is None:
                                delay = full_jitter_delay(
                                    attempt, CFG.retry_base_sec, CFG.retry_cap_sec
                                )
                            await self._sleep_or_stop(delay)
                            continue

                        if response.status in {408, 425, 500, 502, 503, 504}:
                            self.stats.http_errors += 1
                            await self._sleep_or_stop(
                                full_jitter_delay(attempt, CFG.retry_base_sec, CFG.retry_cap_sec)
                            )
                            continue

                        body = await response.text()
                        if response.status != 200:
                            self.stats.http_errors += 1
                            logger.warning(
                                "HTTP %s for %s: %s",
                                response.status,
                                response.url.with_query(""),
                                body[:300],
                            )
                            if 400 <= response.status < 500:
                                return None
                            await self._sleep_or_stop(
                                full_jitter_delay(attempt, CFG.retry_base_sec, CFG.retry_cap_sec)
                            )
                            continue

                        try:
                            data = json.loads(body)
                        except json.JSONDecodeError:
                            self.stats.http_errors += 1
                            await self._sleep_or_stop(
                                full_jitter_delay(attempt, CFG.retry_base_sec, CFG.retry_cap_sec)
                            )
                            continue

                        if not isinstance(data, dict):
                            return None

                        if data.get("status") == "0" and not is_no_transactions(data):
                            classification = classify_api_error(data)
                            if classification == "fatal":
                                raise FatalAPIError(api_error_text(data))
                            if classification == "retryable":
                                self.stats.soft_errors += 1
                                await self._sleep_or_stop(
                                    full_jitter_delay(
                                        attempt, CFG.retry_base_sec, CFG.retry_cap_sec
                                    )
                                )
                                continue
                        return data

            except FatalAPIError:
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                self.stats.http_errors += 1
                if attempt + 1 == CFG.max_request_retries:
                    logger.debug("Request exhausted retries: %r", exc)
                    break
                await self._sleep_or_stop(
                    full_jitter_delay(attempt, CFG.retry_base_sec, CFG.retry_cap_sec)
                )
        return None

    async def _sleep_or_stop(self, delay: float) -> None:
        try:
            await asyncio.wait_for(self.stop_event.wait(), timeout=max(0.0, delay))
        except asyncio.TimeoutError:
            pass

    async def fetch_action(self, address: str, action: str) -> FetchOutcome:
        result: list[dict[str, Any]] = []
        page = 1
        while not self.stop_event.is_set():
            if CFG.max_pages_per_address and page > CFG.max_pages_per_address:
                return FetchOutcome(
                    ok=False,
                    error=f"MAX_PAGES_PER_ADDRESS exceeded for {action}",
                )

            data = await self._request(build_params(address, action, page))
            if data is None:
                return FetchOutcome(ok=False, error=f"request failed for {action} page {page}")
            if is_no_transactions(data):
                return FetchOutcome(ok=True, transactions=result)

            page_rows = data.get("result")
            if not isinstance(page_rows, list):
                return FetchOutcome(
                    ok=False,
                    error=f"unexpected result for {action}: {api_error_text(data)[:300]}",
                )

            result.extend(row for row in page_rows if isinstance(row, dict))
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
    logger.info("Scanning existing output: %s", path)
    async with aiofiles.open(path, "r", encoding="utf-8") as file:
        async for line in file:
            try:
                row = json.loads(line)
                record_id = row.get("record_id")
                if record_id:
                    ids.add(str(record_id))
                elif row.get("hash"):
                    # Compatibility with v13 normal-transaction output.
                    ids.add(f"normal:{str(row['hash']).lower()}")
            except (json.JSONDecodeError, AttributeError, TypeError):
                continue
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

                transactions = request.transactions
                new_db_ids = store.insert_transaction_ids(tx.record_id for tx in transactions)
                to_append = [tx for tx in transactions if tx.record_id not in output_ids]

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

                inserted = len(new_db_ids)
                stats.tx_written += inserted
                stats.tx_duplicates += len(transactions) - inserted
                if not request.done.done():
                    request.done.set_result(inserted)
            except Exception as exc:
                if request is not None and not request.done.done():
                    request.done.set_exception(exc)
                raise
            finally:
                queue.task_done()


# =============================================================================
# CRAWL WORKERS
# =============================================================================


def parse_transactions(rows: list[dict[str, Any]], stats: Stats) -> tuple[list[Transaction], set[str]]:
    records: list[Transaction] = []
    neighbors: set[str] = set()

    for tx in rows:
        tx_hash = str(tx.get("hash", "")).strip()
        if not tx_hash:
            continue
        from_addr = normalize_address(tx.get("from"))
        to_addr = normalize_address(tx.get("to"))
        if not from_addr or not to_addr:
            continue
        if CFG.skip_self_transfers and from_addr == to_addr:
            continue

        kind = str(tx.get("_kind", "normal"))
        value_wei = str(tx.get("value", "0") or "0")
        records.append(
            Transaction(
                record_id=transaction_record_id(tx, kind),
                hash=tx_hash,
                from_addr=from_addr,
                to_addr=to_addr,
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
        neighbors.add(to_addr)

    stats.tx_observed += len(records)
    return records, neighbors


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
        try:
            if stop_event.is_set():
                # Leave durable status as queued for the next run.
                continue
            if not store.mark_in_progress(item.address):
                continue

            outcome = await client.fetch_transactions(item.address)
            if not outcome.ok:
                retry = store.mark_retry_or_failed(item, outcome.error)
                if retry and not stop_event.is_set():
                    await work_queue.put(item)
                else:
                    stats.addresses_failed += 1
                continue

            records, neighbors = parse_transactions(outcome.transactions, stats)
            if records:
                future: asyncio.Future[int] = asyncio.get_running_loop().create_future()
                await writer_queue.put(WriteRequest(records, future))
                await future

            if item.depth > 1:
                for neighbor in neighbors:
                    if store.enqueue(neighbor, item.depth - 1):
                        await work_queue.put(WorkItem(neighbor, item.depth - 1))

            store.mark_done(item.address)
            stats.addresses_done += 1

        except FatalAPIError as exc:
            logger.error("Fatal Etherscan API error: %s", exc)
            store.mark_retry_or_failed(item, str(exc))
            fatal_event.set()
            stop_event.set()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Worker %s failed for %s", worker_id, item.address)
            retry = store.mark_retry_or_failed(item, repr(exc))
            if retry and not stop_event.is_set():
                await work_queue.put(item)
            else:
                stats.addresses_failed += 1
        finally:
            work_queue.task_done()


async def monitor_loop(
    store: StateStore,
    work_queue: asyncio.Queue[WorkItem],
    writer_queue: asyncio.Queue[Optional[WriteRequest]],
    stop_event: asyncio.Event,
    stats: Stats,
) -> None:
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=CFG.log_every_sec)
            return
        except asyncio.TimeoutError:
            pass
        counts = store.counts()
        elapsed = max(0.001, time.monotonic() - stats.started_monotonic)
        logger.info(
            "addr done=%s queued=%s active=%s failed=%s | work_q=%s writer_q=%s | "
            "tx observed=%s new=%s dup=%s | req=%s http_err=%s soft_err=%s | %.2f tx/s",
            counts["done"], counts["queued"], counts["in_progress"], counts["failed"],
            work_queue.qsize(), writer_queue.qsize(), stats.tx_observed,
            stats.tx_written, stats.tx_duplicates, stats.requests,
            stats.http_errors, stats.soft_errors, stats.tx_observed / elapsed,
        )


# =============================================================================
# ORCHESTRATION
# =============================================================================


async def run() -> int:
    CFG.validate()
    start = normalize_address(CFG.start_address)
    if not start:
        raise RuntimeError("START_ADDRESS is not a valid EVM address")

    if CFG.reset_state:
        CFG.output_file.unlink(missing_ok=True)
        CFG.state_db.unlink(missing_ok=True)
        Path(str(CFG.state_db) + "-wal").unlink(missing_ok=True)
        Path(str(CFG.state_db) + "-shm").unlink(missing_ok=True)

    store = StateStore(CFG.state_db)
    stop_event = asyncio.Event()
    fatal_event = asyncio.Event()
    stats = Stats()

    loop = asyncio.get_running_loop()

    def request_shutdown() -> None:
        if not stop_event.is_set():
            logger.warning("Shutdown requested; current work will be checkpointed")
            stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, request_shutdown)
        except (NotImplementedError, RuntimeError):
            # Windows Proactor loops may not support add_signal_handler.
            pass

    try:
        store.recover()
        store.enqueue(start, CFG.depth)
        queued = store.queued_items()
        if not queued:
            logger.info("No queued addresses. Crawl is already complete for this state DB.")
            return 0

        # The dynamically expanding BFS queue must remain unbounded: a bounded queue can
        # deadlock when every worker is simultaneously adding newly discovered neighbors.
        work_queue: asyncio.Queue[WorkItem] = asyncio.Queue()
        writer_queue: asyncio.Queue[Optional[WriteRequest]] = asyncio.Queue(
            CFG.writer_queue_size
        )
        for item in queued:
            await work_queue.put(item)

        output_ids = await load_output_ids(CFG.output_file)
        # Populate SQLite IDs from existing output for v13 -> v14 migration.
        store.insert_transaction_ids(output_ids)

        timeout = aiohttp.ClientTimeout(
            total=CFG.request_timeout_sec,
            connect=CFG.connect_timeout_sec,
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
            "User-Agent": "etherscan-bfs-crawler/14.0",
        }
        limiter = TokenBucket(CFG.rate_limit_per_sec, CFG.burst_size)
        semaphore = asyncio.Semaphore(CFG.concurrent_requests)

        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers=headers,
        ) as session:
            client = EtherscanClient(
                session, semaphore, limiter, stop_event, stats
            )
            writer_task = asyncio.create_task(
                writer_loop(store, writer_queue, output_ids, stats),
                name="writer",
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
            done, pending = await asyncio.wait(
                {join_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
            )

            if join_task in done:
                stop_event.set()

            # Workers may be blocked in queue.get() after a normal drain, or may still be
            # processing during shutdown. SQLite recovery returns in-progress work to the
            # queue on the next launch, so cancellation is safe here.
            for worker in workers:
                worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

            # Persist all requests already accepted by the writer before stopping it.
            await writer_queue.join()
            await writer_queue.put(None)
            await writer_task

            monitor_task.cancel()
            await asyncio.gather(monitor_task, return_exceptions=True)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

        counts = store.counts()
        elapsed = max(0.001, time.monotonic() - stats.started_monotonic)
        logger.info(
            "Done | complete=%s queued=%s active=%s failed=%s | tx_new=%s dup=%s | "
            "requests=%s | elapsed=%.1fs",
            counts["done"], counts["queued"], counts["in_progress"], counts["failed"],
            stats.tx_written, stats.tx_duplicates, stats.requests, elapsed,
        )
        return 2 if fatal_event.is_set() else 0
    finally:
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
