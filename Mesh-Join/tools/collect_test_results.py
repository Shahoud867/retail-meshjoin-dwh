"""Run the whole test suite and record the real result, so it is never written by hand.

Writes Project-Report/evidence/test_results.json, and the results table in ReadMe/TESTING.md.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT.parent / "Project-Report" / "evidence" / "test_results.json"
TESTING_MD = ROOT.parent / "ReadMe" / "TESTING.md"
BEGIN = "<!-- results:begin -->"
END = "<!-- results:end -->"
LAYERS = (
    ("Unit", "tests/unit"),
    ("Integration (real MySQL)", "tests/integration"),
    ("OLAP against independent answers", "tests/oracle"),
    ("End to end and failure (real program)", "tests/e2e"),
)


def run_layer(path: str) -> dict:
    started = datetime.now()
    done = subprocess.run(
        [sys.executable, "-m", "pytest", path, "-q", "--no-header", "--tb=line"],
        check=False,
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    tail = done.stdout.strip().splitlines()[-1] if done.stdout.strip() else ""
    counts = {
        name: int(number)
        for number, name in re.findall(r"(\d+) (passed|failed|skipped|errors?)", tail)
    }
    return {
        "path": path,
        "exit_code": done.returncode,
        "summary": tail.strip("= "),
        "counts": counts,
        "seconds": round((datetime.now() - started).total_seconds()),
    }


def main() -> int:
    layers = [{"name": name, **run_layer(path)} for name, path in LAYERS]
    for layer in layers:
        print(layer["name"], "->", layer["summary"])
    results = {
        "recorded_at": datetime.now().isoformat(timespec="seconds"),
        "layers": layers,
        "passed": sum(layer["counts"].get("passed", 0) for layer in layers),
        "failed": sum(
            layer["counts"].get("failed", 0)
            + layer["counts"].get("error", 0)
            + layer["counts"].get("errors", 0)
            for layer in layers
        ),
        "skipped": sum(layer["counts"].get("skipped", 0) for layer in layers),
    }
    EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    EVIDENCE.write_text(json.dumps(results, indent=1), encoding="utf-8")

    lines = [
        f"Recorded {results['recorded_at']}: **{results['passed']} passed, "
        f"{results['failed']} failed, {results['skipped']} skipped.**",
        "",
        "| Layer | Passed | Failed | Skipped | Seconds |",
        "|---|---:|---:|---:|---:|",
    ]
    for layer in layers:
        counts = layer["counts"]
        failed = counts.get("failed", 0) + counts.get("error", 0) + counts.get("errors", 0)
        lines.append(
            f"| {layer['name']} | {counts.get('passed', 0)} | {failed} | "
            f"{counts.get('skipped', 0)} | {layer['seconds']} |"
        )
    text = TESTING_MD.read_text(encoding="utf-8")
    block = f"{BEGIN}\n" + "\n".join(lines) + f"\n{END}"
    TESTING_MD.write_text(
        re.sub(re.escape(BEGIN) + r".*?" + re.escape(END), lambda _: block, text, flags=re.S),
        encoding="utf-8",
    )
    return 1 if results["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
