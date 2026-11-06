"""OnlineThread against a real file and a real buffer, with no database."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from retail_dw.config import Settings
from retail_dw.control import RuntimeControl
from retail_dw.meshjoin.stream_buffer import StreamBuffer
from retail_dw.metrics import ONLINE_BAD_LINES, Metrics
from retail_dw.threads.online_thread import (
    ONLINE_COLUMNS,
    OnlineThread,
    count_online_rows,
)


def write_file(path: Path, rows: int, extra: str = "") -> Path:
    lines = [",".join(ONLINE_COLUMNS)]
    lines += [
        f"{500000 + i},{12000 + i},P{i},2,1.25,2011-01-01 10:00:00,Online" for i in range(rows)
    ]
    path.write_text("\n".join(lines) + "\n" + extra, encoding="utf-8")
    return path


class Consumer:
    """Empties the buffer the way MESHJOIN would, and remembers what it took."""

    def __init__(self, buffer: StreamBuffer) -> None:
        self.buffer = buffer
        self.taken: list = []

    def drain(self) -> int:
        batch = self.buffer.take(500)
        self.taken.extend(batch)
        return len(batch)


def start(path: Path, control: RuntimeControl, offset: int = 0):
    buffer = StreamBuffer()
    metrics = Metrics()
    thread = OnlineThread(path, buffer, control, metrics, start_offset=offset)
    thread.start()
    return thread, buffer, metrics


def finish(thread: OnlineThread, control: RuntimeControl) -> None:
    control.stop_readers.set()
    thread.join(5)
    assert not thread.is_alive()


@pytest.mark.parametrize(("tps", "low", "high"), [(80, 60, 100), (10, 6, 15)])
def test_rate_stays_near_the_setting(tmp_path: Path, tps: int, low: int, high: int) -> None:
    path = write_file(tmp_path / "s.csv", 5000)
    settings = Settings(peak_tps=tps)
    control = RuntimeControl(settings)
    thread, buffer, _ = start(path, control)
    consumer = Consumer(buffer)
    began = time.monotonic()
    while time.monotonic() - began < 2.0:
        consumer.drain()
        time.sleep(0.02)
    consumer.drain()
    finish(thread, control)
    rate = len(consumer.taken) / 2.0
    assert low <= rate <= high, rate


def test_rate_change_applies_at_once(tmp_path: Path) -> None:
    path = write_file(tmp_path / "s.csv", 20000)
    control = RuntimeControl(Settings(peak_tps=20, non_peak_tps=1000, start_speed="peak"))
    thread, buffer, _ = start(path, control)
    consumer = Consumer(buffer)
    time.sleep(1.0)
    slow = consumer.drain()
    control.set_speed("non-peak")
    took = 0
    began = time.monotonic()
    while time.monotonic() - began < 1.0:
        took += consumer.drain()
        time.sleep(0.02)
    finish(thread, control)
    assert took > slow * 5


def test_pause_stops_reading_and_resume_continues_in_order(tmp_path: Path) -> None:
    path = write_file(tmp_path / "s.csv", 2000)
    control = RuntimeControl(Settings(peak_tps=500, start_mode="periodic"))
    thread, buffer, _ = start(path, control)
    time.sleep(0.5)
    assert buffer.counts().total == 0
    control.set_mode("both")
    time.sleep(0.5)
    consumer = Consumer(buffer)
    consumer.drain()
    assert consumer.taken
    finish(thread, control)
    ids = [t.transaction_id for t in consumer.taken]
    assert ids == [str(500000 + i) for i in range(len(ids))]


def test_end_of_file_is_reported_and_every_row_arrives(tmp_path: Path) -> None:
    path = write_file(tmp_path / "s.csv", 300)
    control = RuntimeControl(Settings(peak_tps=5000))
    thread, buffer, metrics = start(path, control)
    consumer = Consumer(buffer)
    while thread.is_alive():
        consumer.drain()
        time.sleep(0.01)
    consumer.drain()
    assert control.online_done.is_set()
    assert len(consumer.taken) == 300
    assert count_online_rows(path) == 300
    assert metrics.total("online_read") == 300
    assert consumer.taken[0].processing_mode == "Live"


def test_resume_offset_skips_exactly_the_committed_rows(tmp_path: Path) -> None:
    path = write_file(tmp_path / "s.csv", 100)
    control = RuntimeControl(Settings(peak_tps=5000))
    thread, buffer, _ = start(path, control, offset=40)
    consumer = Consumer(buffer)
    while thread.is_alive():
        consumer.drain()
        time.sleep(0.01)
    consumer.drain()
    assert len(consumer.taken) == 60
    assert consumer.taken[0].transaction_id == str(500040)


def test_bad_lines_are_counted_and_do_not_stop_the_stream(tmp_path: Path) -> None:
    bad = "broken,line\n1,2,3,notanumber,1.0,2011-01-01 10:00:00,Online\n"
    path = write_file(tmp_path / "s.csv", 50, extra=bad)
    with path.open("a", encoding="utf-8") as handle:
        handle.write("500999,12999,P9,1,1.00,2011-01-01 10:00:00,Online\n")
    control = RuntimeControl(Settings(peak_tps=5000))
    thread, buffer, metrics = start(path, control)
    consumer = Consumer(buffer)
    while thread.is_alive():
        consumer.drain()
        time.sleep(0.01)
    consumer.drain()
    assert len(consumer.taken) == 51
    assert metrics.total(ONLINE_BAD_LINES) == 2
    assert control.fatal_error is None


def test_wrong_header_is_fatal(tmp_path: Path) -> None:
    path = tmp_path / "wrong.csv"
    path.write_text("a,b,c\n", encoding="utf-8")
    control = RuntimeControl(Settings())
    thread, _, _ = start(path, control)
    thread.join(5)
    assert control.fatal_error is not None
    assert control.stop_all.is_set()


def test_missing_file_is_fatal(tmp_path: Path) -> None:
    control = RuntimeControl(Settings())
    thread, _, _ = start(tmp_path / "absent.csv", control)
    thread.join(5)
    assert isinstance(control.fatal_error, OSError)


def test_full_buffer_waits_and_stop_unblocks(tmp_path: Path) -> None:
    path = write_file(tmp_path / "s.csv", 5000)
    control = RuntimeControl(Settings(peak_tps=100000))
    thread, buffer, _ = start(path, control)
    time.sleep(0.5)
    assert buffer.counts().total == 1000
    assert thread.is_alive()
    finish(thread, control)
    assert buffer.counts().total == 1000
