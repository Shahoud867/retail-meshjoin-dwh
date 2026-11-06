"""Download the raw dataset from UCI when the user agrees to it.

The dataset is not redistributed with the project, so a fresh checkout may not have
it. The SHA-256 check means the totals in the report hold for whatever was downloaded,
or the download is refused.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
import zipfile
from collections.abc import Callable
from pathlib import Path, PurePosixPath
from typing import BinaryIO
from urllib.request import Request, urlopen

from retail_dw.prepare.cleaning import DataFileError

UCI_ZIP_URL = "https://archive.ics.uci.edu/static/public/352/online+retail.zip"
RAW_FILE_NAME = "Online Retail.xlsx"
RAW_FILE_SHA256 = "43465a06f2ccf7c8b5bd2892bc7defb52f97487934fe93b16ae4c3936424676d"

# Generic on purpose: nothing about the user or the machine leaves with the request.
_USER_AGENT = "Mozilla/5.0"
_TIMEOUT_SECONDS = 60
_BLOCK_BYTES = 1024 * 1024
# The real file is about 24 MB in both forms. The limit stops a wrong URL, or a zip
# that expands without end, from filling the disk.
_MAX_FILE_BYTES = 200 * 1024 * 1024


def ensure_raw_file(path: str | Path, confirm: Callable[[str], bool]) -> Path:
    """Return the path of the raw file, downloading it first if needed and allowed.

    confirm receives a question and returns whether the user agreed. An existing file
    is used as it is. Nothing appears at path unless its checksum matched.
    """
    target = Path(path)
    if target.is_file():
        return target
    if not confirm(f"{target} was not found. Download it from {UCI_ZIP_URL}?"):
        raise DataFileError(f"Raw data file not found: {target}")
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        # In the target's folder, so the final replace is one atomic rename.
        with tempfile.TemporaryDirectory(dir=target.parent, prefix=".download-") as work:
            archive = Path(work) / "download.zip"
            extracted = Path(work) / "extracted.xlsx"
            _download(archive)
            _extract_raw_file(archive, extracted)
            _verify_checksum(extracted)
            os.replace(extracted, target)
    except (OSError, zipfile.BadZipFile) as exc:
        raise DataFileError(f"Could not download the raw data file: {exc}") from exc
    return target


def _download(destination: Path) -> None:
    request = Request(UCI_ZIP_URL, headers={"User-Agent": _USER_AGENT})
    with urlopen(request, timeout=_TIMEOUT_SECONDS) as response, destination.open("wb") as sink:
        _copy_limited(response, sink)


def _extract_raw_file(archive: Path, destination: Path) -> None:
    """Copy the one expected member to a path chosen here. Member names are never used
    as paths, so a name such as ../../x cannot write outside the work folder."""
    with zipfile.ZipFile(archive) as bundle:
        for member in bundle.infolist():
            if PurePosixPath(member.filename).name == RAW_FILE_NAME:
                with bundle.open(member) as source, destination.open("wb") as sink:
                    _copy_limited(source, sink)
                return
    raise DataFileError(f"The downloaded archive does not contain {RAW_FILE_NAME}")


def _copy_limited(source: BinaryIO, sink: BinaryIO) -> None:
    copied = 0
    while block := source.read(_BLOCK_BYTES):
        copied += len(block)
        if copied > _MAX_FILE_BYTES:
            raise DataFileError(f"Download is larger than the {_MAX_FILE_BYTES} byte limit")
        sink.write(block)


def _verify_checksum(path: Path) -> None:
    with path.open("rb") as handle:
        actual = hashlib.file_digest(handle, "sha256").hexdigest()
    if actual != RAW_FILE_SHA256:
        raise DataFileError(
            f"Downloaded {RAW_FILE_NAME} has SHA-256 {actual}, expected {RAW_FILE_SHA256}"
        )
