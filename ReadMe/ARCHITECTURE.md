# Architecture

For a developer who needs to change or extend this system. Operating steps are in `README.md`.

## System

```text
 Online Retail.xlsx
        |  prepare (once, at startup)
        v
 Cleaning -> Rules -+-> online_sales_stream.csv --> OnlineThread ----+
                    +-> periodic_sales_data ------> PeriodicThread --+
                    +-> customer_ and product_master_data            v
                                  |                   Stream buffer (limit 1,000)
                                  |                   lanes: online > periodic > shed
        +------------------------ | ---- MeshJoinThread ------------ | --------+
        |                         v                                  v         |
        |  LoadShedder     Disk buffers (500 + 500)  --probe-->  Hash table H  |
        |    ^    |                                                 Queue      |
        |    |    v                                                   |        |
        |  shed_table                              consolidate + Store_list    |
        |                                                             |        |
        |            Live: live_cache, flushed when slow <------------+        |
        |            Periodic and Shed: held until the hour tick <----+        |
        |                               |                                      |
        |              Loader: dimensions first, then fact_sales               |
        +-------------------------------|--------------------------------------+
                                        v
                        Star schema --> Queries-DW, STORE_QUARTERLY_SALES
                                        |
                                        +--> optional read-only dashboard (own process)

 Main thread: prompts, command console, shutdown, row accounting
```

## The star

```text
    dim_customer       dim_product
           \             /
 dim_store --- fact_sales --- dim_supplier
                   |
               dim_time
```

Grain: one fact row is one product line in one cart. `transaction_id` stays on the fact as a degenerate dimension, because Q16 needs it. `sales_key` is a surrogate primary key because `(transaction_id, product_id)` is not unique: 19,157 cleaned rows share that pair with another row. Supplier links only through the fact, so this stays a star rather than a snowflake. `time_key` is `YYYYMMDDHHMM`, so it needs no lookup.

All id columns use `utf8mb4_bin`, so database comparison and order match Python strings exactly.

## Code map (`Mesh-Join/retail_dw`)

| Module | Responsibility |
|---|---|
| `config.py` | PDF section 12 constants, `Settings`, `ConnectionConfig`, flags |
| `control.py` | Mode, speed, simulated hour, stop signals; thread-safe |
| `metrics.py` | Run and hourly counters |
| `db.py` | One session per thread; `run_transaction` with deadlock and lost-connection handling |
| `prepare/` | `rules` (MD5 rules, pure), `cleaning`, `master_data`, `split`, `fetch`, `pipeline` |
| `meshjoin/` | `engine` (the four steps, no database and no clock), `stream_buffer`, `hash_table`, `chunk_queue`, `disk_buffer`, `load_shedder`, `model` |
| `load/` | `consolidate` (revenue, store, time), `loader`, `live_cache`, `sink` (routing) |
| `threads/` | `OnlineThread`, `PeriodicThread`, `MeshJoinThread` |
| `console.py`, `app.py` | Prompts, banner, commands, startup order, shutdown |
| `reconcile.py` | Row accounting read from the database |
| `dashboard/` | Optional read-only web view, separate process |

## Thread model

Exactly three workers (PDF section 4.4), and the main thread only handles the console.

- `OnlineThread` reads the file lazily and paces itself. It has no database session.
- `PeriodicThread` owns the hour clock and admits group rows to the buffer.
- `MeshJoinThread` runs the join, shedding and all warehouse writes. As the only writer of dimensions and facts, it cannot race on a dimension insert, so there is no fourth loader thread.

They share no turn. The shared state is the `StreamBuffer` (one lock), `RuntimeControl`, `Metrics` and the `StagingGate`, each held only briefly. Each thread has its own database session.

## MESHJOIN with two tables

Each iteration reads one partition of each table, adds up to 500 transactions to H, probes every Master Data tuple into H, and removes the oldest chunk if its visits cover both tables. `H` has two multi-maps, by customer and by product, over the same transaction objects, so nothing is copied.

A chunk lives `max(Nc, Np)` = 9 iterations. It therefore meets all 9 customer and all 8 product partitions, whatever its entry point. The repeated product visit overwrites the same values, and output happens once, at chunk removal.

There is no Python dictionary of Master Data anywhere: tuples exist in memory only as the current partition, and probing always runs from a Master Data tuple into H.

Memory is bounded regardless of data size: Stream buffer 1,000, H 4,500, disk buffers 500 each, held periodic results about 25,000.

## Load shedding

MESHJOIN's capacity is well above the average arrival rate, so a pure rate test would never shed. The real pressure is the burst: a group of about 2,480 rows arrives at once at a 1,000-slot buffer. So:

1. At a tick, `PeriodicThread` opens the group and admits what fits, keeping `ONLINE_RESERVE_SLOTS` free so that online rows are never locked out.
2. **Peak:** at its next iteration, `MeshJoinThread` moves the rest to `shed_table` with one `INSERT ... SELECT`, disk to disk. If the buffer is full, that is the whole group.
3. **Non-peak:** nothing is shed. `PeriodicThread` tops up every 200 ms.
4. **Return:** at non-peak in `both` or `periodic`, with no group rows waiting and room in the buffer, the oldest shed rows return on the lowest-priority lane.

The `StagingGate` lock makes admission and shedding exclusive, so a row can only take one route, and a new group always gets its chance at the buffer first. Only rows of `periodic_sales_data` can reach `shed_table`, and a `CHECK` enforces that too.

## Where results go

| Result | Route |
|---|---|
| Live, cache mode | `live_cache`, then the fact in a slow period, in one transaction with the delete |
| Live, direct mode | The fact, straight away |
| Periodic and Shed | Held in memory, loaded at the next hour tick (PDF sections 7, 16) |
| No Master Data match | `etl_reject` with a reason; never partly loaded |

## Recovery

Every write transaction records its progress in the same commit:

- `etl_progress.online_rows_committed` advances with each commit of Live rows. Online rows commit in file order, so this is an exact offset into the file.
- `periodic_sales_data.loaded` is set in the same transaction as the fact insert.
- `etl_progress.txn_seq` is raised in every write transaction. If a connection is lost at COMMIT, reading it back with a locking read shows whether that commit happened, so work is neither repeated nor lost. If it cannot be determined, the run stops rather than guessing.

`--resume` returns staging rows that are released, not loaded and not on the shed disk to `released = 0` (they were in memory), and restarts online at the saved offset. A row re-joined this way after being shed gets `processing_mode = Periodic`, because its earlier route was lost with the memory.

## Security

- The password is typed at startup (`getpass`), and held only in `ConnectionConfig`, whose repr hides it. There is no flag or environment variable for it in the program.
- All values are parameterised. The database name cannot be a parameter, so it is validated against `[A-Za-z0-9_]{1,64}` and quoted.
- The dashboard binds to 127.0.0.1, checks the Host header, uses read-only sessions, and can only run fixed SQL or the graded queries by id.
- Before dropping anything, the program checks that the database is its own.

## Known limitations

- Cleaning follows the PDF's closed list, so 5,194 exact duplicate rows stay, and sales later cancelled stay at full value. The dropped cancellation lines are worth 611,342.09, 6.86% of cleaned revenue.
- Stores 2 to 5 are skewed by the assignment's hash rule. Remainders 7 and 9 are odd, so `% 4` can only be 1 or 3; remainder 8 is even, so it can only be 0 or 2. Measured carts: 909, 1,869, 920, 1,863.
- MESHJOIN gives every row a fixed wait of a full cycle, even when it matched early.
- Master Data is assumed not to change during a run; if it does, the run stops.
- Connection details are entered by hand, and one run uses one source file.

## Extending it

- **A real stream:** replace `OnlineThread`'s file reader with a message queue consumer. Everything after the `StreamBuffer` is unchanged. Keep the offset commit in the same transaction.
- **A real ODS:** load `periodic_sales_data` from change data capture in place of `prepare/split.py`.
- **A smarter join:** implement another algorithm behind the same engine interface (for example HYBRIDJOIN, which adapts to skew).
- **Larger Master Data:** partitions already read by key range, so cost per iteration stays flat, though chunk life grows with the partition count.
