"""Build the submission zip of PDF section 13, with exactly its six folders.

    python tools/package_submission.py

The raw dataset, logs, caches, local secrets and virtual environments are left out. The zip
is checked after it is written: every required file must be in it, and nothing excluded.
"""

from __future__ import annotations

import sys
import zipfile
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[2]
NAME = "ShahoudShahid_23i2515_Project"
FOLDERS = ("Create-DW", "Mesh-Join", "Queries-DW", "Data", "Project-Report", "ReadMe")
REQUIRED = (
    "Create-DW/create_dw.sql",
    "Mesh-Join/main.py",
    "Mesh-Join/requirements.txt",
    "Queries-DW/queries_dw.sql",
    "Data/online_sales_stream.csv",
    "Data/customer_master_data.csv",
    "Data/product_master_data.csv",
    "Data/DATA_NOTE.md",
    "Project-Report/Project_Report.pdf",
    "ReadMe/README.md",
)
EXCLUDED_DIRS = {"__pycache__", ".pytest_cache", ".ruff_cache", "logs", "raw", ".venv", "dist"}
EXCLUDED_NAMES = {".env"}
# The report is submitted as one PDF. Its Word and LaTeX working files stay out of the zip.
EXCLUDED_PATHS = {
    "Project-Report/Project_Report.docx",
    "Project-Report/Project_Report.tex",
    "Project-Report/Project_Report_LaTeX.pdf",
    "Project-Report/source/report_template.tex",
    "Project-Report/source/build_latex.py",
}
EXCLUDED_PREFIXES = ("Project-Report/figures/",)
EXCLUDED_SUFFIXES = {".pyc", ".log"}


def included(path: Path) -> bool:
    relative = path.relative_to(PROJECT)
    if any(part in EXCLUDED_DIRS for part in relative.parts):
        return False
    posix = relative.as_posix()
    if posix in EXCLUDED_PATHS or posix.startswith(EXCLUDED_PREFIXES):
        return False
    return path.name not in EXCLUDED_NAMES and path.suffix not in EXCLUDED_SUFFIXES


def main() -> int:
    missing = [name for name in REQUIRED if not (PROJECT / name).is_file()]
    if missing:
        print("Missing required files:", ", ".join(missing))
        return 1
    target = PROJECT / "dist" / f"{NAME}.zip"
    target.parent.mkdir(exist_ok=True)
    count = 0
    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for folder in FOLDERS:
            for path in sorted((PROJECT / folder).rglob("*")):
                if path.is_file() and included(path):
                    archive.write(path, f"{NAME}/{path.relative_to(PROJECT).as_posix()}")
                    count += 1
    with zipfile.ZipFile(target) as archive:
        names = archive.namelist()
    top = {name.split("/")[1] for name in names}
    assert top == set(FOLDERS), top
    for name in REQUIRED:
        assert f"{NAME}/{name}" in names, name
    assert not any(n.endswith((".tex", ".docx")) or "/Project-Report/figures/" in n for n in names)
    assert not any("/raw/" in n or n.endswith((".env", ".pyc", ".log")) for n in names)
    print(f"Wrote {target}: {count} files, {target.stat().st_size / 1e6:.1f} MB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
