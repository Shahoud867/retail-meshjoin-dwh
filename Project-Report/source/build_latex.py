"""Fill report_template.tex from the evidence files, then compile it with latexmk.

    python Project-Report/source/build_latex.py

Writes Project-Report/Project_Report.tex and Project_Report.pdf (LaTeX build).
"""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import build_report as br  # noqa: E402  (module-level code only loads the evidence)

OUT = br.REPORT_DIR


def esc(value) -> str:
    text = str(value)
    for old, new in (("\\", r"\textbackslash{}"), ("&", r"\&"), ("%", r"\%"), ("$", r"\$"),
                     ("#", r"\#"), ("_", r"\_"), ("{", r"\{"), ("}", r"\}"),
                     ("~", r"\textasciitilde{}"), ("^", r"\textasciicircum{}")):
        text = text.replace(old, new)
    return text


def table(spec: str, header: list[str], rows: list[list], caption: str, label: str | None = None,
          long: bool = False) -> str:
    head = " & ".join(rf"\textbf{{{esc(h)}}}" for h in header) + r" \\"
    body = "\n".join(" & ".join(esc(c) for c in r) + r" \\" for r in rows)
    lab = rf"\label{{{label}}}" if label else ""
    if long:
        return (rf"\begin{{longtable}}{{{spec}}}" "\n"
                rf"\caption{{{caption}}}{lab}\\" "\n" rf"\toprule {head} \midrule \endfirsthead" "\n"
                rf"\toprule {head} \midrule \endhead" "\n" r"\bottomrule \endfoot" "\n"
                f"{body}\n" r"\end{longtable}")
    return (r"\begin{table}[ht]\centering" "\n" rf"\caption{{{caption}}}{lab}" "\n"
            rf"\begin{{tabular}}{{{spec}}}" "\n" rf"\toprule {head} \midrule" "\n" f"{body}\n"
            r"\bottomrule\end{tabular}" "\n" r"\end{table}")


def t_shed_summary() -> str:
    shed = br.ev("shed_by_speed")
    rows = [[k.capitalize(), f"{shed[k]['hours']:,}", f"{shed[k]['shed_out']:,}",
             f"{shed[k]['returned']:,}", f"{shed[k]['shed_rows_loaded']:,}"] for k in ("peak", "non-peak")]
    return table("lrrrr", ["Hours at", "Hours", "Shed to disk", "Returned", "Shed rows loaded"],
                 rows, "Shedding and return, by the speed at the start of each hour.")


def t_groups() -> str:
    return table("rlrrrrr", ["Hour", "Speed", "Group", "Group size", "Admitted", "Shed", "Returned"],
                 [[str(c) for c in r] for r in br.shed_hour_rows()],
                 "Release, admission, shedding and return, hour by hour.", long=True)


def t_routes() -> str:
    return table("llrr", ["sales_mode", "processing_mode", "Transactions", "Revenue"], br.route_rows(),
                 "Rows in the fact table, by route.")


def t_left_over() -> str:
    left = br.ev("left_over")
    return (rf"Left on the shed disk: {left['shed']}. Left in the Live cache: {left['live_cache']}. "
            rf"Rejected: {left['rejected']}. Unreleased in staging: {left['unreleased']}.")


def t_spikes() -> str:
    rows = [[r["product_id"], r["product_name"], r["day"], f"{r['revenue']:,.2f}",
             f"{r['average']:,.2f}", f"{r['times']:.1f}"] for r in br.ev("q19.largest")]
    return table(r"l>{\raggedright\arraybackslash}p{4.4cm}lrrr", ["Product", "Name", "Day", "Revenue", "Own average", "Times"], rows,
                 "The five largest Q19 spikes relative to the product's own average.", "tab:spikes")


def t_timings() -> str:
    q = br.OLAP["queries"]
    half = (len(q) + 1) // 2
    rows = []
    for i in range(half):
        left = [q[i]["query"], f"{q[i]['rows']:,}", str(q[i]["seconds"])]
        right = ([q[i + half]["query"], f"{q[i + half]['rows']:,}", str(q[i + half]["seconds"])]
                 if i + half < len(q) else ["", "", ""])
        rows.append(left + right)
    return table("lrrlrr", ["Query", "Rows", "Seconds", "Query", "Rows", "Seconds"], rows,
                 "Rows returned and run time of each query.")


def t_tests_intro() -> str:
    t = br.TESTS
    return (rf"The final test run recorded {t['passed']:,} tests passed, {t['failed']} failed and "
            rf"{t['skipped']} skipped.")


def t_tests() -> str:
    rows = [[la["name"], la["counts"].get("passed", 0), la["counts"].get("failed", 0),
             la["counts"].get("skipped", 0)] for la in br.TESTS["layers"]]
    return table("lrrr", ["Layer", "Passed", "Failed", "Skipped"], rows, "Results of the final test run.")


GENERATED = {"shed_summary": t_shed_summary, "groups": t_groups, "routes": t_routes,
             "left_over": t_left_over, "spikes": t_spikes, "timings": t_timings,
             "tests_intro": t_tests_intro, "tests": t_tests}


def fill(match: re.Match) -> str:
    kind, arg = match.group(1), match.group(2)
    if kind == "T":
        return GENERATED[arg]()
    if kind == "n":
        return esc(br.num(arg))
    if kind == "e":
        return esc(br.ev(arg))
    if kind == "f3":
        return f"{float(br.ev(arg)):,.3f}"
    if kind == "m":
        return br.spike_month(arg)
    if kind == "e2":
        key = arg.split(".", 1)[1]
        return str(br.OLAP.get(key))
    raise ValueError(match.group(0))


def main() -> int:
    source = (HERE / "report_template.tex").read_text(encoding="utf-8")
    tex = re.sub(r"<<(\w+):([^>]+)>>", fill, source)
    if br.missing:
        print("Evidence missing:", sorted(set(br.missing)))
        return 1
    OUT.joinpath("Project_Report.tex").write_text(tex, encoding="utf-8")
    link = OUT / "figures"
    if not link.exists():
        shutil.copytree(br.FIGURES, link)
    latexmk = shutil.which("latexmk") or "latexmk"
    done = subprocess.run([latexmk, "-pdf", "-interaction=nonstopmode", "-halt-on-error",
                           "-jobname=Project_Report_LaTeX", "Project_Report.tex"], cwd=OUT,
                          capture_output=True, text=True, check=False)
    print(done.stdout[-1500:] if done.returncode else "Compiled.")
    print(done.stderr[-800:])
    return done.returncode


if __name__ == "__main__":
    sys.exit(main())
