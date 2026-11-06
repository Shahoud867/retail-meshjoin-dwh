"""Section 12 values must be exact and fixed, and the password must stay private."""

from __future__ import annotations

import ast
import dataclasses
from pathlib import Path

import pytest

from retail_dw import config
from retail_dw.config import (
    ConnectionConfig,
    Settings,
    build_arg_parser,
    parse_settings,
)


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("PARTITION_SIZE", 500),
        ("CHUNK_SIZE_W", 500),
        ("STREAM_BUFFER_LIMIT", 1000),
        ("PERIODIC_GROUPS", 48),
        ("DEFAULT_PEAK_TPS", 80),
        ("DEFAULT_NON_PEAK_TPS", 10),
        ("DEFAULT_SECONDS_PER_HOUR", 60.0),
        ("ONLINE_RESERVE_SLOTS", 100),
        ("MIN_ITERATION_SECONDS", 0.2),
        ("HELD_PERIODIC_CAP", 20000),
        ("CACHE_FLUSH_SLICE", 2000),
        ("PERIODIC_LOAD_SLICE", 5000),
        ("TOPUP_INTERVAL_SECONDS", 0.2),
    ],
)
def test_constant_values(name: str, value: float) -> None:
    assert getattr(config, name) == value


def test_defaults_with_no_arguments() -> None:
    settings = parse_settings([])
    assert settings == Settings()
    assert settings.peak_tps == 80
    assert settings.non_peak_tps == 10
    assert settings.seconds_per_hour == 60.0
    assert settings.start_speed == "peak"
    assert settings.start_mode == "both"
    assert settings.live_load == "cache"
    assert settings.resume is False
    assert settings.log_dir.parts[-2:] == ("Mesh-Join", "logs")


def test_every_flag_reaches_settings(tmp_path: Path) -> None:
    settings = parse_settings(
        [
            "--peak-rate", "200",
            "--non-peak-rate", "5",
            "--seconds-per-hour", "1.5",
            "--start-speed", "non-peak",
            "--start-mode", "periodic",
            "--live-load", "direct",
            "--resume",
            "--log-dir", str(tmp_path),
        ]
    )  # fmt: skip
    assert settings == Settings(
        peak_tps=200,
        non_peak_tps=5,
        seconds_per_hour=1.5,
        start_speed="non-peak",
        start_mode="periodic",
        live_load="direct",
        resume=True,
        log_dir=tmp_path,
    )


@pytest.mark.parametrize(
    "argv",
    [
        pytest.param(["--peak-rate", "0"], id="zero-peak"),
        pytest.param(["--peak-rate", "-1"], id="negative-peak"),
        pytest.param(["--peak-rate", "fast"], id="text-peak"),
        pytest.param(["--peak-rate", "1.5"], id="fractional-peak"),
        pytest.param(["--non-peak-rate", "0"], id="zero-non-peak"),
        pytest.param(["--non-peak-rate", "-3"], id="negative-non-peak"),
        pytest.param(["--seconds-per-hour", "0"], id="zero-hour"),
        pytest.param(["--seconds-per-hour", "-60"], id="negative-hour"),
        pytest.param(["--seconds-per-hour", "nan"], id="nan-hour"),
        pytest.param(["--seconds-per-hour", "inf"], id="infinite-hour"),
        pytest.param(["--start-speed", "turbo"], id="bad-speed"),
        pytest.param(["--start-mode", "neither"], id="bad-mode"),
        pytest.param(["--live-load", "maybe"], id="bad-live-load"),
    ],
)
def test_invalid_values_exit_with_a_usage_error(
    argv: list[str], capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as exit_info:
        parse_settings(argv)
    assert exit_info.value.code == 2
    assert "usage:" in capsys.readouterr().err


@pytest.mark.parametrize(
    "kwargs",
    [
        pytest.param({"peak_tps": 0}, id="peak"),
        pytest.param({"non_peak_tps": -1}, id="non-peak"),
        pytest.param({"seconds_per_hour": 0.0}, id="hour"),
        pytest.param({"seconds_per_hour": float("nan")}, id="nan"),
        pytest.param({"seconds_per_hour": float("inf")}, id="inf"),
        pytest.param({"start_speed": "PEAK"}, id="speed"),
        pytest.param({"start_mode": "all"}, id="mode"),
        pytest.param({"live_load": "none"}, id="live-load"),
    ],
)
def test_settings_validate_without_the_command_line(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        Settings(**kwargs)


def test_settings_are_immutable() -> None:
    with pytest.raises(dataclasses.FrozenInstanceError):
        Settings().peak_tps = 1  # type: ignore[misc]


@pytest.mark.parametrize(
    "flag",
    [
        "--password",
        "--pass",
        "--pwd",
        "--partition-size",
        "--chunk-size",
        "--stream-buffer-limit",
        "--periodic-groups",
    ],
)
def test_passwords_and_section_12_values_have_no_flag(flag: str) -> None:
    with pytest.raises(SystemExit):
        parse_settings([flag, "1"])


def test_flag_list_is_exactly_the_contract() -> None:
    flags = {
        option
        for action in build_arg_parser()._actions
        for option in action.option_strings
    }
    assert flags == {
        "-h",
        "--help",
        "--peak-rate",
        "--non-peak-rate",
        "--seconds-per-hour",
        "--start-speed",
        "--start-mode",
        "--live-load",
        "--resume",
        "--log-dir",
    }


def test_abbreviated_flags_are_refused() -> None:
    with pytest.raises(SystemExit):
        parse_settings(["--peak", "100"])


class TestConnectionConfig:
    SECRET = "s3cret-Value!"

    @pytest.fixture
    def cfg(self) -> ConnectionConfig:
        return ConnectionConfig("db.example", 3306, "retail", "etl_user", self.SECRET)

    def test_fields_keep_the_contract_order(self, cfg: ConnectionConfig) -> None:
        assert [f.name for f in dataclasses.fields(cfg)] == [
            "host",
            "port",
            "database",
            "user",
            "password",
        ]
        assert cfg.password == self.SECRET

    @pytest.mark.parametrize("render", [repr, str, "{}".format, "{!r}".format])
    def test_rendering_hides_the_password(self, cfg: ConnectionConfig, render) -> None:
        text = render(cfg)
        assert self.SECRET not in text
        assert "***" in text
        assert "etl_user" in text

    def test_repr_does_not_reveal_an_empty_password(self) -> None:
        empty = ConnectionConfig("h", 1, "d", "u", "")
        assert "password='***'" in repr(empty)

    def test_is_immutable(self, cfg: ConnectionConfig) -> None:
        with pytest.raises(dataclasses.FrozenInstanceError):
            cfg.password = "other"  # type: ignore[misc]


def test_config_module_cannot_read_the_environment() -> None:
    """A password environment variable would need os, so it must not be imported."""
    tree = ast.parse(Path(config.__file__).read_text(encoding="utf-8"))
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    assert "os" not in imported
