# Testing

## How to run the tests

From `Mesh-Join`, with the development packages installed:

```bash
pip install -r requirements-dev.txt
```

Unit tests need nothing else:

```bash
python -m pytest tests/unit
```

The other layers need a MySQL server you can freely create databases on. They only create and drop databases named `t_...`. Connection details come from environment variables, so nothing secret is in a file:

```bash
export DWH_TEST_HOST=127.0.0.1
export DWH_TEST_PORT=3307
export DWH_TEST_USER=root
read -s DWH_TEST_PASSWORD && export DWH_TEST_PASSWORD

python -m pytest tests/integration
python -m pytest tests/oracle
python -m pytest tests/e2e
```

The throwaway Docker server from `README.md` works well for this. Without these variables, the database tests are skipped with a clear reason. `tests/oracle` and `tests/e2e` need `Data/raw/Online Retail.xlsx` where noted.

`python tools/collect_test_results.py` runs everything and rewrites the table below.

## Layers

| Layer | What it proves |
|---|---|
| `tests/unit` | MD5 rules against known values; every value list and its order; cleaning, group sizing, description ties; the Stream buffer under real threads; the MESHJOIN engine; commands and prompts; stream pacing |
| `tests/integration` | Real MySQL: schema, keys and constraints, script run twice, Master Data reads, loader, cache, sink, lost connections, deadlocks, real-file preparation, the dashboard |
| `tests/oracle` | All 20 queries, row by row with exact decimals, against answers computed independently in pandas with its own reimplementation of the rules, not from SQL or the project's code |
| `tests/e2e` | The real program as a child process, typed at through stdin: every mode, shedding, recovery and failure |

## Failure scenarios tested on the real program

| Scenario | Expected and checked |
|---|---|
| Process killed outright, twice, mid-run, with rows on the shed disk and in the cache; then `--resume` | Every row exactly once: the fact table is compared row by row with the source |
| All database connections killed repeatedly during a run | Nothing lost or doubled; the run finishes by itself |
| Master Data deleted mid-run | A clear error and exit code 1; after restoring it, `--resume` finishes exactly |
| Wrong password | Clear message, exit code 1, password not in output or logs |
| `--resume` on an unprepared database | Refused with a clear message |
| Database holding another project's `fact_sales` | Refused without a person to confirm; that table is untouched |
| Unrelated tables in the chosen database | Left alone by a normal start |
| Second start after a first run, with the view present | Everything rebuilt |

## What is checked for the engine

- Every transaction is enriched by both passes, for every combination of the two cursors' positions (9 × 8).
- A test fails if a chunk leaves one iteration early, with the row whose partition is seen last.
- Exactly one output per transaction, and output order equals arrival order.
- Memory limits hold over a long randomised run: H ≤ 4,500, Queue ≤ 9, disk buffers ≤ 500.
- Last partition smaller than 500, a table exactly divisible by 500, a single-partition table, and an empty stream.

## Known answers checked against the full warehouse

| Check | Expected |
|---|---|
| Fact rows, quantity, revenue | 397,884; 5,167,812; 8,911,407.904 |
| Online rows, revenue | 278,740; 6,075,186.073 |
| Periodic rows, revenue | 119,144; 2,836,221.831 |
| Customers, products, suppliers, stores, time rows | 4,338; 3,665; 5; 5; 17,282 |
| Q16 top pairs | (22386, 85099B) 546 carts; (22697, 22699) 541; (22726, 22727) 530; (20725, 22384) 523; (20725, 22383) 519 |
| Q19 spike days | 22,885 across 2,975 products |

## A defect the checks caught in my own numbers

An early floating-point profile of the data gave 22,887 spike days for Q19. The exact-arithmetic oracle and the SQL both gave 22,885. 34 product-days sit at exactly twice their average, and floating point misjudged two. Q19 now compares by multiplying, with no rounded average, and 22,885 is the correct figure.

## Results

<!-- results:begin -->
Recorded 2026-11-05T22:25:08: **1256 passed, 0 failed, 1 skipped.**

| Layer | Passed | Failed | Skipped | Seconds |
|---|---:|---:|---:|---:|
| Unit | 762 | 0 | 0 | 25 |
| Integration (real MySQL) | 363 | 0 | 1 | 292 |
| OLAP against independent answers | 114 | 0 | 0 | 144 |
| End to end and failure (real program) | 17 | 0 | 0 | 670 |
<!-- results:end -->
