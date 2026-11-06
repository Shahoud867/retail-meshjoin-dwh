"""The optional download, with the network replaced by an in-memory archive."""

from __future__ import annotations

import hashlib
import io
import zipfile
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request

import pytest

from retail_dw.prepare import fetch
from retail_dw.prepare.cleaning import DataFileError
from retail_dw.prepare.fetch import RAW_FILE_NAME, UCI_ZIP_URL, ensure_raw_file

WORKBOOK = b"pretend this is a workbook"


def archive_with(members: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        for name, content in members.items():
            bundle.writestr(name, content)
    return buffer.getvalue()


class FakeNetwork:
    """Stands in for urlopen and records what was asked for."""

    def __init__(self, payload: bytes | Exception) -> None:
        self.payload = payload
        self.requests: list[Request] = []
        self.timeouts: list[float] = []

    def __call__(self, request: Request, timeout: float) -> io.BytesIO:
        self.requests.append(request)
        self.timeouts.append(timeout)
        if isinstance(self.payload, Exception):
            raise self.payload
        return io.BytesIO(self.payload)


@pytest.fixture
def expect_workbook(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fetch, "RAW_FILE_SHA256", hashlib.sha256(WORKBOOK).hexdigest())


def use_network(monkeypatch: pytest.MonkeyPatch, payload: bytes | Exception) -> FakeNetwork:
    network = FakeNetwork(payload)
    monkeypatch.setattr(fetch, "urlopen", network)
    return network


def must_not_ask(_question: str) -> bool:
    raise AssertionError("the user must not be asked")


def agree(_question: str) -> bool:
    return True


def test_existing_file_is_used_without_asking_or_downloading(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    network = use_network(monkeypatch, b"")
    path = tmp_path / RAW_FILE_NAME
    path.write_bytes(b"anything")
    assert ensure_raw_file(path, must_not_ask) == path
    assert path.read_bytes() == b"anything"
    assert network.requests == []


def test_declining_raises_and_downloads_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    network = use_network(monkeypatch, b"")
    questions: list[str] = []

    def decline(question: str) -> bool:
        questions.append(question)
        return False

    path = tmp_path / RAW_FILE_NAME
    with pytest.raises(DataFileError, match="not found"):
        ensure_raw_file(path, decline)
    assert network.requests == []
    assert not path.exists()
    assert UCI_ZIP_URL in questions[0]


@pytest.mark.usefixtures("expect_workbook")
def test_download_extracts_the_checked_file_and_leaves_nothing_else(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    network = use_network(monkeypatch, archive_with({RAW_FILE_NAME: WORKBOOK, "other.txt": b"x"}))
    path = tmp_path / "new" / "folder" / RAW_FILE_NAME
    assert ensure_raw_file(path, agree) == path
    assert path.read_bytes() == WORKBOOK
    assert list(path.parent.iterdir()) == [path]
    assert network.timeouts[0] > 0


@pytest.mark.usefixtures("expect_workbook")
def test_request_uses_https_and_a_generic_user_agent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    network = use_network(monkeypatch, archive_with({RAW_FILE_NAME: WORKBOOK}))
    ensure_raw_file(tmp_path / RAW_FILE_NAME, agree)
    request = network.requests[0]
    assert request.full_url == UCI_ZIP_URL
    assert request.full_url.startswith("https://")
    assert request.get_header("User-agent") == "Mozilla/5.0"


@pytest.mark.usefixtures("expect_workbook")
def test_member_inside_a_folder_is_found(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    use_network(monkeypatch, archive_with({f"data/{RAW_FILE_NAME}": WORKBOOK}))
    path = tmp_path / RAW_FILE_NAME
    ensure_raw_file(path, agree)
    assert path.read_bytes() == WORKBOOK


def test_wrong_checksum_leaves_no_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    use_network(monkeypatch, archive_with({RAW_FILE_NAME: b"a different workbook"}))
    path = tmp_path / RAW_FILE_NAME
    with pytest.raises(DataFileError, match="SHA-256"):
        ensure_raw_file(path, agree)
    assert list(tmp_path.iterdir()) == []


def test_real_checksum_is_the_one_in_the_build_spec() -> None:
    assert fetch.RAW_FILE_SHA256 == (
        "43465a06f2ccf7c8b5bd2892bc7defb52f97487934fe93b16ae4c3936424676d"
    )


def test_network_failure_is_a_clear_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    use_network(monkeypatch, URLError("no route to host"))
    with pytest.raises(DataFileError, match="no route to host"):
        ensure_raw_file(tmp_path / RAW_FILE_NAME, agree)
    assert list(tmp_path.iterdir()) == []


def test_response_that_is_not_a_zip_is_a_clear_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    use_network(monkeypatch, b"<html>this is an error page</html>")
    with pytest.raises(DataFileError, match="Could not download"):
        ensure_raw_file(tmp_path / RAW_FILE_NAME, agree)
    assert list(tmp_path.iterdir()) == []


def test_archive_without_the_file_is_a_clear_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    use_network(monkeypatch, archive_with({"readme.txt": b"nothing here"}))
    with pytest.raises(DataFileError, match=RAW_FILE_NAME):
        ensure_raw_file(tmp_path / RAW_FILE_NAME, agree)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.usefixtures("expect_workbook")
def test_member_name_cannot_write_outside_the_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    use_network(monkeypatch, archive_with({f"../../{RAW_FILE_NAME}": WORKBOOK}))
    folder = tmp_path / "a" / "b" / "c"
    path = folder / RAW_FILE_NAME
    ensure_raw_file(path, agree)
    assert list(folder.iterdir()) == [path]
    assert not (tmp_path / "a" / RAW_FILE_NAME).exists()
    assert not (tmp_path / RAW_FILE_NAME).exists()


def test_oversized_download_is_stopped(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fetch, "_MAX_FILE_BYTES", 10)
    use_network(monkeypatch, b"x" * 11)
    with pytest.raises(DataFileError, match="limit"):
        ensure_raw_file(tmp_path / RAW_FILE_NAME, agree)
    assert list(tmp_path.iterdir()) == []


def test_member_that_expands_past_the_limit_is_stopped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A small archive can hold a huge file, so the member is limited on its own."""
    payload = archive_with({RAW_FILE_NAME: b"y" * 100_000})
    assert len(payload) < 10_000
    monkeypatch.setattr(fetch, "_MAX_FILE_BYTES", 10_000)
    use_network(monkeypatch, payload)
    with pytest.raises(DataFileError, match="limit"):
        ensure_raw_file(tmp_path / RAW_FILE_NAME, agree)
    assert list(tmp_path.iterdir()) == []
