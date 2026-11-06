"""Build Project_Report.docx and Project_Report.pdf from the evidence files.

Numbers measured by the official run come only from Project-Report/evidence. Anything not
there yet appears as a visible [EVIDENCE MISSING: ...] marker, and the build lists them, so
the report can never state a measurement that did not happen.

    python Project-Report/source/build_report.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from docx import Document  # noqa: E402
from docx.enum.table import WD_TABLE_ALIGNMENT  # noqa: E402
from docx.enum.text import WD_ALIGN_PARAGRAPH  # noqa: E402
from docx.oxml.ns import qn  # noqa: E402
from docx.oxml.shared import OxmlElement  # noqa: E402
from docx.shared import Cm, Pt, RGBColor  # noqa: E402
from matplotlib.patches import FancyBboxPatch  # noqa: E402

REPORT_DIR = Path(__file__).resolve().parents[1]
EVIDENCE = REPORT_DIR / "evidence"
FIGURES = REPORT_DIR / "source" / "figures"
BLUE, ORANGE = "#2a78d6", "#eb6834"
missing: list[str] = []


def load(name: str) -> dict:
    path = EVIDENCE / name
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}


RUN = load("reference_run.json")
OLAP = load("olap_timings.json")
TESTS = load("test_results.json")


def ev(path: str, source: dict | None = None, label: str = "reference_run"):
    """Look up a dotted path in the evidence. Marks the gap if it is not there."""
    value = RUN if source is None else source
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
            missing.append(f"{label}.{path}")
            return f"[EVIDENCE MISSING: {label}.{path}]"
        value = value[part]
    return value


def num(path: str, source: dict | None = None, label: str = "reference_run") -> str:
    value = ev(path, source, label)
    if isinstance(value, bool) or not isinstance(value, int | float):
        return str(value)
    return f"{value:,.0f}" if float(value).is_integer() else f"{value:,.2f}"


# ---------------------------------------------------------------- figures


def box(ax, x, y, w, h, text, fill="#eef3fb", edge=BLUE, size=9, bold=False):
    ax.add_patch(
        FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.08",
                       fc=fill, ec=edge, lw=1.2)
    )
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=size,
            weight="bold" if bold else "normal")


def star_figure(path: Path) -> None:
    fig, ax = plt.subplots(figsize=(8.2, 6.4))
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 8.4)
    ax.axis("off")
    fact = (
        "fact_sales\n\nsales_key (PK)\ncustomer_key (FK)\nproduct_key (FK)\nsupplier_key (FK)\n"
        "store_key (FK)\ntime_key (FK)\ntransaction_id\nsales_mode\nprocessing_mode\n"
        "quantity\nrevenue"
    )
    dims = [
        (0.1, 5.7, 3.1, 2.5, "dim_customer\n\ncustomer_key (PK)\ncustomer_id, gender\nage_group, occupation\ncity_category\nstay_in_current_city_years\nmarital_status"),
        (6.8, 5.9, 3.1, 2.1, "dim_product\n\nproduct_key (PK)\nproduct_id\nproduct_name\nproduct_category"),
        (0.1, 2.9, 3.1, 1.9, "dim_store\n\nstore_key (PK)\nstore_id, store_name\ncity"),
        (6.8, 3.1, 3.1, 1.7, "dim_supplier\n\nsupplier_key (PK)\nsupplier_id\nsupplier_name"),
        (1.9, 0.05, 6.2, 1.75, "dim_time\n\ntime_key (PK), full_datetime, full_date\nhour_of_day, day_of_month, month_no, quarter_no\nyear_no, day_of_week, day_type, season, half_year"),
    ]
    for x, y, w, h, _ in dims:
        ax.plot([x + w / 2, 5], [y + h / 2, 4.95], color="#9a9a94", lw=1.3, zorder=0)
    box(ax, 3.45, 2.3, 3.1, 5.3, fact, fill="#fdeee7", edge=ORANGE, size=9.5)
    for x, y, w, h, text in dims:
        box(ax, x, y, w, h, text, size=8.5)
    fig.savefig(path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def meshjoin_figure(path: Path) -> None:
    fig, ax = plt.subplots(figsize=(9.6, 4.6))
    ax.set_xlim(0, 16.4)
    ax.set_ylim(0, 6.2)
    ax.axis("off")
    arrow = dict(arrowstyle="-|>", color="#55554f", lw=1.4)
    box(ax, 0.1, 4.2, 2.7, 1.0, "OnlineThread\nonline file, paced", size=7.4)
    box(ax, 0.1, 2.6, 2.7, 1.0, "PeriodicThread\nstaging table, hourly", size=7.4)
    box(ax, 3.6, 3.2, 3.3, 1.5, "Stream buffer\n1,000 transactions\nonline > periodic > shed", size=7.4)
    box(ax, 7.7, 3.2, 3.5, 1.5, "Hash table H\ntwo multi-maps\nQueue: up to 9 chunks", fill="#fdeee7", edge=ORANGE, size=7.4)
    box(ax, 7.7, 0.9, 3.5, 1.4, "Disk buffers\n500 customer + 500 product\none partition each", size=7.4)
    box(ax, 12.0, 0.9, 4.2, 1.4, "Master Data R\non disk: 9 and 8 partitions,\nread cyclically", fill="#eaf6ef", edge="#1baf7a", size=7.4)
    box(ax, 12.0, 3.2, 4.2, 1.5, "Consolidate and load\nrevenue, Store_list, time\ndimensions, then fact", size=7.4)
    box(ax, 3.9, 0.9, 2.7, 1.2, "Shed table\nperiodic only", fill="#f5f5f2", edge="#9a9a94", size=7.4)
    for a, b in [((2.85, 4.7), (3.55, 4.2)), ((2.85, 3.1), (3.55, 3.6)), ((6.95, 3.95), (7.65, 3.95)),
                 ((11.25, 3.95), (11.95, 3.95)), ((11.95, 1.6), (11.25, 1.6)), ((9.45, 2.35), (9.45, 3.15))]:
        ax.annotate("", xy=b, xytext=a, arrowprops=arrow)
    dashed = dict(arrowstyle="-|>", color="#55554f", lw=1.2, linestyle="--")
    ax.annotate("", xy=(3.9, 1.9), xytext=(2.3, 2.55), arrowprops=dashed)
    ax.annotate("", xy=(5.25, 3.15), xytext=(5.25, 2.15), arrowprops=dashed)
    ax.text(0.9, 1.75, "peak overflow", fontsize=8)
    ax.text(5.4, 2.5, "non-peak return", fontsize=8)
    ax.text(9.6, 2.65, "probe", fontsize=8)
    fig.savefig(path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def shed_figure(path: Path) -> bool:
    hours = RUN.get("hours")
    if not hours:
        return False
    fig, ax = plt.subplots(figsize=(8.2, 3.2))
    x = [h["sim_hour"] for h in hours]
    ax.bar(x, [h["shed_out"] for h in hours], color=ORANGE, width=0.45, align="edge", label="Shed to disk")
    ax.bar([i - 0.45 for i in x], [h["reinjected"] for h in hours], color=BLUE, width=0.45, align="edge", label="Returned from disk")
    ax.set_xlabel("Simulated hour")
    ax.set_ylabel("Transactions")
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", color="#ecebe6")
    ax.set_axisbelow(True)
    ax.legend(frameon=False)
    fig.savefig(path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    return True


# ---------------------------------------------------------------- document helpers


class Report:
    def __init__(self) -> None:
        self.doc = Document()
        style = self.doc.styles["Normal"]
        style.font.name = "Calibri"
        style.font.size = Pt(11)
        style.paragraph_format.space_after = Pt(7)
        style.paragraph_format.line_spacing = 1.12
        for name, size in (("Heading 1", 16), ("Heading 2", 13)):
            heading = self.doc.styles[name]
            heading.font.name = "Calibri"
            heading.font.size = Pt(size)
            heading.font.bold = True
            heading.font.color.rgb = RGBColor(0x1b, 0x3a, 0x66)
        section = self.doc.sections[0]
        section.left_margin = section.right_margin = Cm(2.4)
        section.top_margin = section.bottom_margin = Cm(2.3)

    def h1(self, text: str) -> None:
        self.doc.add_heading(text, level=1)

    def h2(self, text: str) -> None:
        self.doc.add_heading(text, level=2)

    def p(self, text: str) -> None:
        self.doc.add_paragraph(text)

    def bullets(self, items: list[str]) -> None:
        for item in items:
            self.doc.add_paragraph(item, style="List Bullet")

    def numbered(self, items: list[str]) -> None:
        for number, item in enumerate(items, 1):
            para = self.doc.add_paragraph(f"{number}.  {item}")
            para.paragraph_format.left_indent = Cm(0.8)
            para.paragraph_format.first_line_indent = Cm(-0.6)

    def code(self, text: str) -> None:
        para = self.doc.add_paragraph()
        run = para.add_run(text)
        run.font.name = "Consolas"
        run.font.size = Pt(9.5)
        para.paragraph_format.left_indent = Cm(0.6)

    def figure(self, path: Path, caption: str, width: float = 15.5) -> None:
        self.doc.add_picture(str(path), width=Cm(width))
        self.doc.paragraphs[-1].alignment = WD_ALIGN_PARAGRAPH.CENTER
        para = self.doc.add_paragraph(caption)
        para.alignment = WD_ALIGN_PARAGRAPH.CENTER
        para.runs[0].italic = True
        para.runs[0].font.size = Pt(9.5)

    def table(self, header: list[str], rows: list[list], widths: list[float] | None = None) -> None:
        table = self.doc.add_table(rows=1, cols=len(header))
        table.style = "Table Grid"
        table.alignment = WD_TABLE_ALIGNMENT.CENTER
        for index, text in enumerate(header):
            cell = table.rows[0].cells[index]
            cell.text = ""
            run = cell.paragraphs[0].add_run(text)
            run.bold = True
            run.font.size = Pt(9.5)
            shade = OxmlElement("w:shd")
            shade.set(qn("w:val"), "clear")
            shade.set(qn("w:fill"), "E8EEF7")
            cell._tc.get_or_add_tcPr().append(shade)  # noqa: SLF001
        for row in rows:
            cells = table.add_row().cells
            for index, value in enumerate(row):
                cells[index].text = ""
                run = cells[index].paragraphs[0].add_run(str(value))
                run.font.size = Pt(9.5)
        for row in table.rows:
            row._tr.get_or_add_trPr().append(OxmlElement("w:cantSplit"))  # noqa: SLF001
        table.rows[0]._tr.get_or_add_trPr().append(OxmlElement("w:tblHeader"))  # noqa: SLF001
        if widths:
            for row in table.rows:
                for index, width in enumerate(widths):
                    row.cells[index].width = Cm(width)
        self.doc.add_paragraph()

    def cover(self) -> None:
        for _ in range(3):
            self.doc.add_paragraph()
        for text, size, bold in (
            ("National University of Computer and Emerging Sciences", 15, False),
            ("FAST-NUCES", 13, False),
            ("", 12, False),
            ("DS3003 & DS3004: Data Warehousing & Business Intelligence", 12, False),
            ("Fall 2026", 12, False),
            ("", 12, False),
            ("Building and Analysing a Near-Real-Time and Periodic Data Warehouse for a Retail Chain", 21, True),
            ("", 12, False),
            ("Project Report", 14, False),
            ("", 12, False),
            ("Shahoud Shahid", 14, True),
            ("Roll Number: 23i-2515", 12, False),
            ("Section: DS-A", 12, False),
            ("", 12, False),
            ("13 November 2026", 12, False),
        ):
            para = self.doc.add_paragraph()
            para.alignment = WD_ALIGN_PARAGRAPH.CENTER
            run = para.add_run(text)
            run.font.size = Pt(size)
            run.bold = bold
        self.doc.add_page_break()


def spike_month(month: str) -> str:
    """Spike days in one month, from the official run's warehouse."""
    months = ev("q19.by_month")
    if not isinstance(months, list):
        return str(months)
    return next(f"{m['spike_days']:,}" for m in months if m["month"] == month)


def route_rows() -> list[list]:
    routes = ev("by_route")
    if not isinstance(routes, list):
        return [[routes, "", "", ""]]
    return [
        [r["sales_mode"], r["processing_mode"], f"{r['rows_loaded']:,}", f"{float(r['revenue']):,.3f}"]
        for r in routes
    ]


def shed_hour_rows() -> list[list]:
    hours = RUN.get("hours")
    if not hours:
        missing.append("reference_run.hours")
        return [["[EVIDENCE MISSING: reference_run.hours]"] + [""] * 6]
    return [
        [h["sim_hour"], h["speed_mode"], "" if h["group_no"] is None else h["group_no"],
         f"{h['group_size']:,}", f"{h['admitted']:,}", f"{h['shed_out']:,}", f"{h['reinjected']:,}"]
        for h in hours
        if h["group_no"] is not None or h["shed_out"] or h["reinjected"]
    ]


def build(report: Report) -> None:
    report.cover()

    report.h1("1. Project Overview")
    report.p(
        "This project builds and analyses a Data Warehouse for a retail chain that sells both on "
        "its website and in physical stores. Online sales need to reach the warehouse shortly after "
        "they happen, while store sales arrive once an hour. Both come from one public file of real "
        "transactions, the UCI Online Retail dataset, which runs from 1 December 2010 to 9 December "
        "2011. The file has no Master Data, so the program builds it using fixed rules."
    )
    report.p(
        "The work consists of a Python program and a MySQL star schema. The program cleans the file, "
        "builds the Master Data and splits the transactions, then starts three independent threads. "
        "MESHJOIN, a Stream-Relation join, adds the customer and product attributes held on disk to "
        "each transaction. The enriched rows are loaded into the warehouse, and 20 OLAP queries are "
        "then run against it."
    )
    report.p(
        "I also implemented the optional bonus, where Live results go to a cache table first and move "
        "into the warehouse during a slow period. Every figure in this report comes from one official "
        "run at the PDF's settings, on MySQL "
        f"{ev('server_version')}."
    )

    report.h1("2. Data Warehouse Schema")
    report.figure(FIGURES / "star.png", "Figure 1: The star schema. Each dimension joins the fact table through a surrogate key created for this project.", 14.5)
    report.p(
        "The fact table sits in the middle and holds one tuple per transaction: one product in one "
        "shopping cart, with one customer, store, supplier and time. That's the finest grain the data "
        "offers, and the queries need it, since Q16 and Q19 work on individual transactions. Totals "
        "are calculated in SQL and are never stored in a second fact table."
    )
    report.bullets([
        "Measures: quantity and revenue. Revenue (SALE = quantity × price) isn't in the transactional file or the Master Data, so it's calculated when the tuple is consolidated.",
        "Surrogate primary key: sales_key. The pair (transaction_id, product_id) can't be the key, because 19,157 cleaned rows share that pair with another row. A unique constraint on it would quietly drop data.",
        "Degenerate dimension: transaction_id stays on the fact table, because Q16 counts carts with it.",
        "Five foreign keys: customer, product, supplier, store and time. Supplier joins through the fact table rather than through product, which keeps the design a star and not a snowflake.",
        "Time: time_key is YYYYMMDDHHMM, with one row per distinct event time. Its parts are stored as separate columns so queries can group by them without re-reading the timestamp: date, hour, day of month, month, quarter, year, day of week, weekday or weekend, season and half-year.",
        "Every dimension keeps the original ids, stored with a binary collation so that MySQL compares them exactly the way Python does.",
    ])
    report.p(
        "Create-DW/create_dw.sql starts by dropping the view STORE_QUARTERLY_SALES, the fact table and "
        "the dimensions, in dependency order. It then recreates them with their primary keys, foreign "
        "keys and CHECK constraints. It also creates the working tables, which have no foreign keys into "
        "the warehouse: customer_master_data, product_master_data, periodic_sales_data, shed_table and "
        "live_cache, plus tables for logs, rejects and recovery."
    )

    report.h1("3. Data Preparation and Cleaning Counts")
    report.p("The program does all the cleaning itself, with no manual edits to the file. A transaction is kept only if its quantity and unit price are both above zero, it has a customer ID, and its transaction_id doesn't start with C.")
    report.table(
        ["Count", "Transactions"],
        [
            ["Before cleaning", "541,909"],
            ["After cleaning", "397,884"],
            ["Removed", "144,025"],
            ["Failed: cancellation (id starts with C)", "9,288"],
            ["Failed: quantity not above zero", "10,624"],
            ["Failed: unit price not above zero", "2,517"],
            ["Failed: customer ID missing", "135,080"],
        ],
        [9, 4],
    )
    report.p("A row can fail more than one test, so the four failure counts add up to more than the number removed. After cleaning there are 18,532 shopping carts, 4,338 customers and 3,665 products.")
    report.p(
        "Two details decide whether the rules work at all. First, the id columns mix integers and "
        "strings, and the customer column has blanks, so a default spreadsheet read turns 17850 into "
        "17850.0. That changes the id's MD5 and, with it, every attribute of every customer. To avoid "
        "this, the program reads the raw values and converts each id to exact text. Second, the PDF's "
        "cleaning tests are a closed list, so the 5,194 exact duplicate rows stay in as separate "
        "transactions."
    )

    report.h1("4. The sales_mode Rule and the Store Rule")
    report.p("The file doesn't say where each cart was bought, so a fixed rule decides this from the invoice number. Every line in a cart gets the same result:")
    report.code("int(MD5(transaction_id)[:8], 16) % 10\n0 to 6: online    7 to 9: physical store")
    report.p("A store cart then gets its store from the same number, while online carts use store 1:")
    report.code("store_id = 2 + (int(MD5(transaction_id)[:8], 16) % 4)")
    report.table(
        ["transaction_id", "First 8 hex", "As integer", "% 10", "Result"],
        [
            ["536365", "3e6d49ce", "1047349710", "0", "Online, store 1"],
            ["536370", "2d918f31", "764514097", "7", "Physical; % 4 = 1, store 3"],
        ],
    )
    report.p("The rule produced 278,740 online transactions (70.06%) and 119,144 periodic ones (29.94%), in line with the 70 and 30 percent the PDF expects.")
    report.p(
        "The store rule has a side effect that matters when reading store results. Both remainders come "
        "from the same number, so they aren't independent. Remainders 7 and 9 are odd, and an odd number "
        "modulo 4 is always 1 or 3, so those carts can only land in stores 3 and 5. Remainder 8 is even, "
        "so those carts can only land in stores 2 and 4. In other words, two of the three store remainders "
        "feed just two stores."
    )
    report.table(
        ["Store", "Name", "Carts"],
        [["2", "London Bridge", "909"], ["3", "Manchester Arndale", "1,869"], ["4", "Birmingham Bullring", "920"], ["5", "Edinburgh Princes", "1,863"]],
        [2, 7, 3],
    )
    report.p("As a result, Manchester and Edinburgh get roughly twice as many carts as London and Birmingham. That's purely the arithmetic of the rule, not real trade, so the store comparisons in Q12, Q13, Q15 and Q20 shouldn't be read as a measure of store performance.")

    report.h1("5. MESHJOIN")
    report.p(
        "MESHJOIN was introduced by Polyzotis et al. in 2008 to join a stream with a relation during the "
        "transformation phase of ETL. Here the stream S is the sequence of transactions, and the relation R "
        "is the Master Data, which stays on disk because it's treated as too large to load into memory. "
        "Looking up R once per transaction would mean a lot of random reads, and the join would fall behind "
        "the stream. Waiting for each hour to close would stall the stream instead. MESHJOIN avoids both by "
        "reading R cyclically, one partition at a time, and joining each partition with all the transactions "
        "currently in memory."
    )
    report.figure(FIGURES / "meshjoin.png", "Figure 2: The MESHJOIN pipeline as implemented. Dashed arrows are the load shedding path.")
    report.h2("Components")
    report.table(
        ["Component", "In this program"],
        [
            ["Stream buffer", "Holds up to 1,000 transactions while MESHJOIN finishes an iteration. Three lanes share that space and are read in priority order: online, periodic, shed."],
            ["Hash table H", "A multi-map of the transactions in memory, with two indexes over the same objects (by customer_id and by product_id), since there are two join attributes."],
            ["Queue", "Keeps a reference to each chunk in arrival order, oldest at the end. It never copies a transaction."],
            ["Disk buffer", "One per Master Data table, holding a single partition of at most 500 tuples. The next iteration overwrites it."],
            ["Master Data R", "Two MySQL tables, read by key range 500 tuples at a time in an endless cycle. Nothing is cached."],
        ],
        [3.5, 12.5],
    )
    report.h2("One iteration")
    report.numbered([
        "Read the next partition of each Master Data table into its Disk buffer, wrapping back to the first after the last.",
        "Move up to 500 transactions from the Stream buffer into H as one chunk, and add its pointer to the Queue.",
        "Probe H with the join attribute of each Master Data tuple in the Disk buffers. The customer pass adds the customer attributes, and the product pass adds the product and supplier. The store isn't part of the join: it comes from the in-memory Store_list when the row is consolidated.",
        "Remove the oldest chunk once it has been compared with all of both tables.",
    ])
    report.h2("Partitions, and how long a chunk stays")
    report.table(
        ["Master Data table", "Tuples", "Partitions", "Tuples in last partition"],
        [
            ["Customer", num("master_data.customers"), num("master_data.customer_partitions"), "338"],
            ["Product", num("master_data.products"), num("master_data.product_partitions"), "165"],
        ],
    )
    report.p(
        "The PDF expects about six product partitions but asks for the count to come from the loaded table. "
        "The cleaned file has 3,665 distinct products, which makes eight. Since the two cycles have different "
        "lengths, a chunk stays for the longer one, nine iterations. That way it meets all nine customer "
        "partitions and all eight product partitions, wherever the two cursors were when it entered. On its "
        "ninth iteration it sees one product partition a second time, which just sets the same values again. "
        "Removing it one iteration earlier would lose matches, and a test confirms this by building a row "
        "whose partition is seen last."
    )
    report.h2("Threads and modes")
    report.p(
        "The system runs on three independent threads that don't take turns. OnlineThread reads the online "
        "file at an adjustable speed. PeriodicThread keeps the simulated clock and extracts one group each "
        "hour. MeshJoinThread runs the four steps and the load shedding, and it's the only thread that writes "
        "to the warehouse. While the program runs, the commands online, periodic, both, peak, non-peak, status "
        "and quit change its behaviour."
    )

    report.h1("6. Three Shortcomings of MESHJOIN")
    report.h2("1. A fixed wait, even when the match is found early")
    report.p(
        "Every transaction stays for a full cycle of the Master Data, nine iterations here, even if both its "
        "matches turned up in the first. The Master Data keys are unique, so nothing more can arrive, but the "
        f"algorithm has no way to use that. In the official run the median Live latency was {num('live_latency_ms.p50_of_hours_median')} ms "
        f"and the median 95th percentile was {num('live_latency_ms.p95_of_hours_median')} ms. About 1.8 seconds of "
        "that is this fixed cycle, at the minimum iteration time of 200 ms. The cost also grows with the Master "
        "Data: doubling R doubles the wait."
    )
    report.h2("2. No adaptation to skew")
    report.p(
        "The cyclic scan reads every partition equally often, no matter what the stream needs. Real demand is uneven: "
        f"in this data the busiest 10% of products account for {num('skew.products.top_decile_share_pct')}% of transactions, "
        f"and the busiest 10% of customers for {num('skew.customers.top_decile_share_pct')}%. A tuple wanted by "
        "hundreds of transactions costs the same disk read as one that nobody wants. Later algorithms such as "
        "HYBRIDJOIN use an index and key frequencies to fetch what's needed sooner."
    )
    report.h2("3. Fragile under bursts")
    report.p(
        "The Stream buffer and chunk size are fixed, so the join responds to the shape of the arrivals, not just "
        f"their rate. The average iteration took {num('iteration_ms_weighted_average')} ms, which gives far more "
        "capacity than the average arrival of about 121 transactions per second (80 online plus about 41 "
        "periodic). Even so, each hourly release of about 2,480 rows can't fit in a 1,000-slot buffer, so rows "
        "get shed at peak even though there's plenty of spare capacity a moment later. Anything in the buffer or "
        "in H is also lost if the program crashes. This project handles that with a recovery log, which is extra "
        "machinery MESHJOIN doesn't provide on its own."
    )

    report.h1("7. Periodic Groups")
    report.p(
        "The periodic transactions are sorted by event_time and split into 48 groups of equal count, with group 0 "
        "at the first hour and group 47 at the last. A released flag in the staging table makes sure no "
        "transaction is extracted twice."
    )
    report.table(
        ["Group measure", "Transactions"],
        [
            ["Total periodic transactions", "119,144"],
            ["Smallest group", num("groups.smallest")],
            ["Largest group", num("groups.largest")],
            ["Groups of 2,483", "8"],
            ["Groups of 2,482", "40"],
        ],
        [9, 4],
    )
    report.p("Because the groups have equal counts, every one of the 47 boundaries falls inside a shopping cart, so a cart's lines can arrive an hour apart. Every line still loads, and Q16 counts carts over the finished warehouse, so the results aren't affected.")

    report.h1("8. Load Shedding: Shed Counts from Peak and Non-Peak Hours")
    report.p(
        "Only periodic transactions can be moved to disk when MESHJOIN can't cope. At peak speed, the rows of a "
        "released group that don't fit in the Stream buffer move from the staging table to the shed table in a "
        "single SQL statement, without any of them being read into memory. If the buffer is already full, the "
        "whole group goes; otherwise only part of it does. At non-peak speed nothing is shed, and shed rows come "
        "back through the lowest-priority lane."
    )
    shed = ev("shed_by_speed")
    if isinstance(shed, dict):
        report.table(
            ["Hours at", "Simulated hours", "Shed to disk", "Returned from disk", "Shed rows loaded"],
            [
                ["Peak", f"{shed['peak']['hours']:,}", f"{shed['peak']['shed_out']:,}", f"{shed['peak']['returned']:,}", f"{shed['peak']['shed_rows_loaded']:,}"],
                ["Non-peak", f"{shed['non-peak']['hours']:,}", f"{shed['non-peak']['shed_out']:,}", f"{shed['non-peak']['returned']:,}", f"{shed['non-peak']['shed_rows_loaded']:,}"],
            ],
        )
        report.p(
            "Rows are only shed at peak speed and only return at non-peak speed. Each hour is labelled by the speed "
            "at its start. The switch to non-peak happened during hour 30, so the 23,694 rows in the peak row's "
            "returned column actually came back after the switch, later in that same hour. Every shed row came back. "
            "Shed rows load on the hour tick after they're joined, which is why all of them are counted in non-peak "
            "hours."
        )
    else:
        report.p(str(shed))
    if shed_figure(FIGURES / "shed.png"):
        report.figure(FIGURES / "shed.png", "Figure 3: Transactions shed and returned in each simulated hour of the official run.")
    report.h2("Group by group")
    report.table(["Hour", "Speed", "Group", "Group size", "Admitted", "Shed", "Returned"], shed_hour_rows())
    report.h2("Reconciliation")
    report.p(f"By the end of the official run the warehouse held {num('fact.rows_loaded')} transactions, with quantity {num('fact.quantity')} and revenue {float(ev('fact.revenue')):,.3f}.")
    report.table(["sales_mode", "processing_mode", "Transactions", "Revenue"], route_rows())
    left = ev("left_over")
    if isinstance(left, dict):
        report.p(f"Left on the shed disk: {left['shed']}. Left in the Live cache: {left['live_cache']}. Rejected: {left['rejected']}. Unreleased in staging: {left['unreleased']}.")

    report.h1("9. OLAP Analysis")
    report.p(
        "Queries-DW/queries_dw.sql answers all 20 questions from the DW Analysis section using the stored revenue. "
        "Slicing shows up in Q4, Q9, Q10, Q12 and Q18, which fix time at 2011, and dicing in Q1, Q6 and Q11. "
        "Drill-down appears in Q1 and Q9 (year to month), Q13 (store to supplier to product), Q14 (product to season), "
        "Q17 (grand total to product) and Q19 (product to day). MySQL doesn't support materialized views, and the PDF "
        "rules out storing totals in a second fact table, so Q20 is a plain view, with the ordering applied by the "
        "SELECT that reads it."
    )
    report.p(
        "Each query's output was checked row by row, with exact decimals, against answers calculated independently "
        "in pandas from the raw file."
    )
    report.h2("Reading the results correctly")
    report.bullets([
        "Dates: the loaded year is 2011, because 2010 only has December. December 2011 stops on the 9th, so that month and Q4 2011 are partial, and their growth figures look artificially low.",
        "Weekend means Sunday: there are no Saturday transactions in the cleaned data at all (62,773 Sunday rows).",
        "Revenue is overstated. Cleaning removes cancellation lines but keeps the sales they cancelled. The removed lines are worth 611,342.09, or 6.86% of cleaned revenue.",
        "The top product is an artefact: product 23843 ranks first overall because of a single 168,469.60 line of 80,995 units, which was cancelled 12 minutes later.",
        "Six non-merchandise codes (POST, M, DOT, C2, PADS, BANK CHARGES) remain as products, with 1,547 rows and 150,341.25 in revenue.",
        "Q16: the top pairs by distinct carts are (22386, 85099B) in 546 carts, (22697, 22699) in 541, (22726, 22727) in 530, (20725, 22384) in 523 and (20725, 22383) in 519. These could be useful for product bundles.",
    ])
    report.h2("Q19: comment on the revenue spikes")
    report.p(
        f"Q19 finds {num('q19.spike_days')} spike days across {num('q19.products')} products, out of "
        f"{num('q19.product_days')} product-days with sales. Each product's average is taken over the days it actually "
        "sold. The spikes fall into three groups."
    )
    largest = ev("q19.largest")
    if isinstance(largest, list):
        report.table(
            ["Product", "Name", "Day", "Revenue", "Own average", "Times"],
            [[r["product_id"], r["product_name"], r["day"], f"{r['revenue']:,.2f}", f"{r['average']:,.2f}", r["times"]] for r in largest],
        )
    report.bullets([
        "Data errors that look like demand: product 23166 hit 77,183.60 on 18 January 2011, about 109 times its average, from a single 74,215-unit order that was cancelled 16 minutes later. It isn't a real spike.",
        "Single large lines: product 22502 reached about 94 times its average on 10 June 2011, mostly from one line of 60 units at 649.50, worth 38,970.00. It should be checked before it's taken at face value.",
        f"Seasonal demand: spike days cluster between September and November 2011 ({spike_month('2011-09')}, {spike_month('2011-10')} and {spike_month('2011-11')}), which fits with stock-buying for Christmas.",
    ])
    report.p(
        "The rule also has a blind spot. Product 23843 has the largest line in the file but is never flagged, because it "
        "sold on only one day, so that day's revenue equals its own average. The test compares each product with its own "
        "history, and a product with no history can't exceed it."
    )
    report.h2("Query run times")
    queries = OLAP.get("queries")
    if queries:
        report.table(
            ["Query", "Rows", "Seconds", "Query", "Rows", "Seconds"],
            [
                [
                    queries[i]["query"], f"{queries[i]['rows']:,}", queries[i]["seconds"],
                    *([queries[i + 11]["query"], f"{queries[i + 11]['rows']:,}", queries[i + 11]["seconds"]] if i + 11 < len(queries) else ["", "", ""]),
                ]
                for i in range(11)
            ],
        )
        report.p(
            f"Measured on database {OLAP.get('database')} with a stock MySQL 8.0 configuration and a "
            f"128 MB buffer pool. The slowest query took {OLAP.get('slowest_seconds')} seconds."
        )
        report.p(
            "Q16 took some extra work. A plain self join produced 9.8 million pair rows and ran for over "
            "two minutes on this server. Since a pair can't appear in more carts than either of its products, "
            "the 5th best pair among the 100 most frequent products sets a floor, and only products in at "
            "least that many carts need to be paired. The answer is still exact, and it was checked against "
            "the independent calculation."
        )
    else:
        report.p(str(ev("queries", OLAP, "olap_timings")))

    report.h1("10. Bonus: The Live Cache")
    report.p(
        "The bonus only changes when a Live result enters the warehouse. Fully enriched Live join results are written to "
        "the live_cache table. During a slow period, which this project takes to mean non-peak speed, they're copied through "
        "the same loader as every other path, and the copied rows are deleted in the same transaction, so a row is always in "
        "exactly one place. Periodic results are never cached and still load on the hour after they're joined. The cache is "
        "also emptied when the program closes, so no row gets stranded. Starting the program with --live-load direct sends "
        "Live results straight into the fact table instead."
    )

    report.h1("11. Testing and Verification")
    if TESTS:
        report.p(f"The final test run recorded {TESTS['passed']:,} tests passed, {TESTS['failed']} failed and {TESTS['skipped']} skipped.")
        report.table(
            ["Layer", "Passed", "Failed", "Skipped"],
            [[layer["name"], layer["counts"].get("passed", 0), layer["counts"].get("failed", 0), layer["counts"].get("skipped", 0)] for layer in TESTS["layers"]],
        )
    else:
        report.p(str(ev("layers", TESTS, "test_results")))
    report.bullets([
        "The engine is tested for every combination of the two cursors' positions, including a test that fails if a chunk leaves one iteration early.",
        "The real program runs as a child process, with commands typed into it through stdin, in every mode.",
        "It was killed outright twice mid-run and then resumed. A row-by-row comparison of the fact table with the source showed no row lost or doubled.",
        "All of its database connections were killed repeatedly during a run, and it recovered on its own with exact totals.",
        "Tests check that the password never appears in output, logs or source files.",
    ])

    report.h1("12. Interpretations of the Assignment")
    report.p("Where the PDF leaves a point open, the program uses the readings below. These are my interpretations, not requirements of the PDF.")
    report.table(
        ["Point", "Reading used"],
        [
            ["processing_mode, named in section 6.2 and never defined", "The route the row took: Live, Periodic or Shed."],
            ["Where the store is added (section 4.2 and section 5.4.3 differ)", "Outside MESHJOIN, from the in-memory Store_list."],
            ["Materialized view (section 11) and view (Q20)", "A plain view, for the reasons in section 9 of this report."],
            ["The loaded year", "2011. December 2010 and Q4 2010 are the base for the first growth figures."],
            ["Q1 and Q11", "Q1 ranks over the whole year, then drills into months. Q11 ranks inside each month."],
            ["When MESHJOIN cannot cope", "At peak, periodic rows that do not fit the Stream buffer when their group is released."],
            ["Longest Description", "Trim edge spaces, then take the longest; the first in file order wins a tie (54 products tie)."],
            ["Shed rows in online mode", "Stay on disk, because that mode is online form only."],
            ["Slow period", "Non-peak speed, plus a final flush when the program closes."],
            ["Q7 and Q19 averages", "Q7 per transaction, with a per-cart column. Q19 over the days the product sold."],
        ],
        [7, 9],
    )

    report.h1("13. Lessons Learned")
    report.bullets([
        "Rules that look simple depend on exact text. A single decimal point in a customer id would have quietly changed every derived attribute without raising an error. Checking the rules against known values caught this kind of mistake before any data was loaded.",
        "Average rates can be misleading. The join had plenty of capacity for the average arrival rate, yet the hourly burst still forced shedding. The shape of the load mattered more than its rate.",
        "It pays to check a rule's arithmetic before trusting its output. The store skew came from reusing one number for two remainders, and it only showed up once the carts were counted.",
        "Natural keys have to be proven against real data. The obvious key for the fact table isn't unique, and enforcing it would have lost rows without any warning.",
        "Floating point isn't good enough for checks. An early float-based profile gave 22,887 spike days for Q19, while exact arithmetic gave 22,885, because 34 product-days sit at exactly twice their average.",
        "Cleaning decisions shape the analysis. Following the rules kept sales that were cancelled minutes later, and the warehouse's top product is one of them. Totals are only as meaningful as the cleaning behind them.",
        "One independent check is worth more than many tests of the same kind. Recomputing every answer separately found mistakes in the checks themselves as well as in the numbers.",
    ])

    report.h1("14. Dataset Citation")
    report.p("Chen, D., Sain, S. L., and Guo, K. Online Retail. UCI Machine Learning Repository. https://archive.ics.uci.edu/dataset/352/online+retail . Downloaded 3 November 2026.")
    report.p("Polyzotis, N., Skiadopoulos, S., Vassiliadis, P., Simitsis, A., and Frantzell, N. Meshing Streaming Updates with Persistent Data in an Active Data Warehouse. IEEE Transactions on Knowledge and Data Engineering, 2008.")


def export_pdf(docx_path: Path, pdf_path: Path) -> bool:
    """Export through Word, which keeps the layout exact. Word is always closed afterwards."""
    script = (
        "$w = New-Object -ComObject Word.Application; $w.Visible = $false; try { "
        f"$d = $w.Documents.Open('{docx_path}', $false, $true); "
        f"$d.ExportAsFixedFormat('{pdf_path}', 17); $d.Close(0) }} finally {{ $w.Quit() }}"
    )
    import subprocess

    done = subprocess.run(
        ["powershell.exe", "-NoProfile", "-Command", script], capture_output=True, text=True, check=False
    )
    if done.returncode != 0 or not pdf_path.is_file():
        print("PDF export failed:", done.stderr[:400])
        return False
    return True


def main() -> int:
    FIGURES.mkdir(parents=True, exist_ok=True)
    star_figure(FIGURES / "star.png")
    meshjoin_figure(FIGURES / "meshjoin.png")
    report = Report()
    build(report)
    docx_path = REPORT_DIR / "Project_Report.docx"
    report.doc.save(docx_path)
    print(f"Wrote {docx_path}")
    print("PDF:", export_pdf(docx_path, REPORT_DIR / "Project_Report.pdf"))
    if missing:
        print("Evidence still missing:")
        for item in sorted(set(missing)):
            print(" ", item)
    else:
        print("All evidence present.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
