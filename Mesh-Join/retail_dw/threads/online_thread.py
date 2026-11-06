"""OnlineThread (PDF 4.4 and 5.2): turns the online file into a stream of a chosen speed."""

from __future__ import annotations

import csv
import logging
import threading
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from retail_dw.control import RuntimeControl
from retail_dw.meshjoin.model import PROCESSING_LIVE, Transaction
from retail_dw.meshjoin.stream_buffer import StreamBuffer
from retail_dw.metrics import ONLINE_BAD_LINES, ONLINE_READ, Metrics

logger = logging.getLogger(__name__)

ONLINE_COLUMNS = [
    "transaction_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "event_time",
    "sales_mode",
]

# Rows are sent in small bursts, because Windows cannot sleep precisely for the 12.5 ms
# between two rows at 80 per second. 50 ms is too short for anyone to notice.
_BURST_SECONDS = 0.05
_PAUSE_POLL_SECONDS = 0.05
# After a long stall the stream continues at its rate and does not race to catch up.
_MAX_BEHIND_SECONDS = 1.0


class OnlineFileError(RuntimeError):
    """The online file is missing or does not have the required columns."""


def parse_online_row(row: list[str]) -> Transaction:
    """Build a Transaction from one CSV row. Raises ValueError or InvalidOperation."""
    if len(row) != len(ONLINE_COLUMNS):
        raise ValueError(f"expected {len(ONLINE_COLUMNS)} fields, got {len(row)}")
    tid, customer, product, quantity, price, event_time, sales_mode = row
    if not (tid and customer and product):
        raise ValueError("an id is empty")
    return Transaction(
        tid,
        customer,
        product,
        int(quantity),
        Decimal(price),
        datetime.fromisoformat(event_time),
        sales_mode,
        PROCESSING_LIVE,
    )


def count_online_rows(path: str | Path) -> int:
    """Rows in the online file, not counting the header."""
    with open(path, newline="", encoding="utf-8") as handle:
        return max(0, sum(1 for _ in csv.reader(handle)) - 1)


class OnlineThread(threading.Thread):
    """Reads the file line by line, so the file is never held in memory.

    The rate is read again before every burst, so a typed command applies at once.
    Valid rows are what start_offset counts, matching what the sink commits.
    """

    def __init__(
        self,
        path: str | Path,
        stream_buffer: StreamBuffer,
        control: RuntimeControl,
        metrics: Metrics,
        start_offset: int = 0,
    ) -> None:
        super().__init__(name="OnlineThread")
        self._path = path
        self._buffer = stream_buffer
        self._control = control
        self._metrics = metrics
        self._start_offset = start_offset

    def run(self) -> None:
        try:
            self._stream()
        except Exception as exc:  # noqa: BLE001 - any failure must stop the whole run
            logger.exception("OnlineThread failed")
            self._control.fatal(exc)

    def _stream(self) -> None:
        with open(self._path, newline="", encoding="utf-8") as handle:
            reader = csv.reader(handle)
            header = next(reader, None)
            if header != ONLINE_COLUMNS:
                raise OnlineFileError(f"{self._path} does not have the online file columns")
            skipped = self._skip_committed(reader)
            logger.info("OnlineThread started after %d committed rows", skipped)
            self._send_paced(reader)
        self._control.online_done.set()
        logger.info("OnlineThread reached the end of the online file")

    def _skip_committed(self, reader: object) -> int:
        skipped = 0
        while skipped < self._start_offset:
            row = next(reader, None)  # type: ignore[call-overload]
            if row is None:
                break
            if self._parse(row) is not None:
                skipped += 1
        return skipped

    def _send_paced(self, reader: object) -> None:
        control = self._control
        next_due = time.monotonic()
        while not control.stop_readers.is_set():
            if not control.online_enabled():
                control.stop_readers.wait(_PAUSE_POLL_SECONDS)
                next_due = time.monotonic()
                continue
            tps = control.current_tps()
            burst, at_end = self._read_burst(reader, max(1, round(tps * _BURST_SECONDS)))
            now = time.monotonic()
            for transaction in burst:
                transaction.enqueued_at = now
            accepted = self._buffer.put_online(burst, stop_event=control.stop_readers)
            self._metrics.add(ONLINE_READ, accepted)
            if at_end:
                if accepted == len(burst):
                    return
                # Stopped while the buffer was full, with rows not accepted.
                control.stop_readers.wait()
                return
            next_due += len(burst) / tps
            delay = next_due - time.monotonic()
            if delay > 0:
                control.stop_readers.wait(delay)
            elif delay < -_MAX_BEHIND_SECONDS:
                next_due = time.monotonic()

    def _read_burst(self, reader: object, size: int) -> tuple[list[Transaction], bool]:
        burst: list[Transaction] = []
        while len(burst) < size:
            row = next(reader, None)  # type: ignore[call-overload]
            if row is None:
                return burst, True
            transaction = self._parse(row)
            if transaction is not None:
                burst.append(transaction)
        return burst, False

    def _parse(self, row: list[str]) -> Transaction | None:
        try:
            return parse_online_row(row)
        except (ValueError, InvalidOperation) as exc:
            self._metrics.add(ONLINE_BAD_LINES)
            logger.warning("Skipping a bad line in the online file: %s", exc)
            return None
