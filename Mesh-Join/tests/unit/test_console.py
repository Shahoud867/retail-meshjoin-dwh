"""Commands and startup questions, without threads or a database."""

from __future__ import annotations

import io

import pytest

from retail_dw.config import Settings
from retail_dw.console import (
    command_loop,
    format_status,
    prompt_connection,
    read_password,
    run_command,
)
from retail_dw.control import RuntimeControl
from retail_dw.threads.meshjoin_thread import PipelineStatus


@pytest.fixture
def control() -> RuntimeControl:
    return RuntimeControl(Settings())


def run(line: str, control: RuntimeControl) -> tuple[bool, list[str]]:
    lines: list[str] = []
    keep_going = run_command(line, control, lambda: "STATUS", lines.append)
    return keep_going, lines


@pytest.mark.parametrize("mode", ["online", "periodic", "both"])
def test_mode_commands(control: RuntimeControl, mode: str) -> None:
    assert run(mode, control)[0]
    assert control.mode == mode


def test_online_mode_stops_groups_and_periodic_mode_pauses_the_stream(control) -> None:
    run("online", control)
    assert control.online_enabled() and not control.periodic_enabled()
    run("periodic", control)
    assert control.periodic_enabled() and not control.online_enabled()
    run("both", control)
    assert control.online_enabled() and control.periodic_enabled()


def test_speed_commands_switch_rate_and_what_is_allowed(control) -> None:
    assert control.current_tps() == 80 and control.shed_allowed()
    run("non-peak", control)
    assert control.current_tps() == 10
    assert not control.shed_allowed() and control.shed_processing_allowed()
    run("peak", control)
    assert control.current_tps() == 80
    assert control.shed_allowed() and not control.shed_processing_allowed()


def test_rates_can_change_while_running(control) -> None:
    run("peak 120", control)
    assert control.current_tps() == 120
    run("non-peak 5", control)
    assert control.current_tps() == 5
    run("peak", control)
    assert control.current_tps() == 120


@pytest.mark.parametrize("bad", ["peak 0", "peak -3", "peak fast", "non-peak 1.5"])
def test_bad_rates_change_nothing(control, bad: str) -> None:
    keep_going, lines = run(bad, control)
    assert keep_going
    assert control.current_tps() == 80 and control.speed == "peak"
    assert "whole number" in lines[0]


@pytest.mark.parametrize("bad", ["fly", "status now", "quit please", "online 5", "peak 1 2"])
def test_unknown_commands_show_help_and_continue(control, bad: str) -> None:
    keep_going, lines = run(bad, control)
    assert keep_going
    assert any("Unknown command" in line for line in lines)


def test_quit_status_help_and_blank(control) -> None:
    assert run("quit", control)[0] is False
    assert run("QUIT", control)[0] is False
    assert run("status", control)[1] == ["STATUS"]
    assert "non-peak" in run("help", control)[1][0]
    assert run("   ", control) == (True, [])


def test_shed_processing_needs_periodic_data_in_the_mode(control) -> None:
    run("non-peak", control)
    run("online", control)
    assert not control.shed_processing_allowed()
    assert control.cache_flush_allowed()


def test_command_loop_stops_at_end_of_input(control) -> None:
    command_loop(control, lambda: "S", stdin=io.StringIO("non-peak\n"), out=lambda _: None)
    assert control.speed == "non-peak"


def test_command_loop_stops_at_quit_and_ignores_what_follows(control) -> None:
    command_loop(control, lambda: "S", stdin=io.StringIO("quit\nnon-peak\n"), out=lambda _: None)
    assert control.speed == "peak"


def test_status_text_uses_the_pdf_component_names() -> None:
    text = format_status(
        PipelineStatus(
            mode="both", speed="peak", tps=80, sim_hour=3, groups_released=4, stream_buffer=12,
            stream_limit=1000, stream_online=2, stream_periodic=10, stream_shed=0, hash_table=500,
            queue_chunks=1, customer_partition=2, customer_partitions=9, product_partition=2,
            product_partitions=8, on_shed_disk=7, on_live_cache=9, held_periodic=1, fact_rows=5,
            live_load="cache", state="running",
        )
    )
    for expected in (
        "Mode: both",
        "Speed: peak (80 tps)",
        "Stream buffer: 12/1000",
        "Hash table H: 500 transactions",
        "Queue: 1 chunks",
        "Disk buffer: customer partition 2/9, product partition 2/8",
        "Shed disk: 7 transactions",
    ):
        assert expected in text


class TestPrompts:
    def test_defaults_and_answers(self) -> None:
        answers = io.StringIO("\n3307\nmy_dw\nalice\nsecret\n")
        cfg = prompt_connection(answers, io.StringIO())
        assert (cfg.host, cfg.port, cfg.database, cfg.user, cfg.password) == (
            "localhost", 3307, "my_dw", "alice", "secret",
        )
        assert "secret" not in repr(cfg)

    def test_bad_port_and_database_name_are_asked_again(self) -> None:
        out = io.StringIO()
        answers = io.StringIO("h\nabc\n70000\n3306\nbad name; DROP\ngood\nu\np\n")
        cfg = prompt_connection(answers, out)
        assert cfg.port == 3306 and cfg.database == "good"
        assert "1 to 65535" in out.getvalue()
        assert "letters, digits" in out.getvalue()

    def test_password_read_from_a_pipe_keeps_its_spaces(self) -> None:
        assert read_password(stdin=io.StringIO(" pa ss \n")) == " pa ss "

    def test_terminal_uses_hidden_input(self, monkeypatch: pytest.MonkeyPatch) -> None:
        class Terminal(io.StringIO):
            def isatty(self) -> bool:
                return True

        monkeypatch.setattr("getpass.getpass", lambda prompt: "hidden")
        assert read_password(stdin=Terminal()) == "hidden"
