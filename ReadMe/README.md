# Near-Real-Time and Periodic Data Warehouse for a Retail Chain

**Student:** Shahoud Shahid | **Roll number:** 23i-2515 | **Section:** DS-A
**Course:** DS3003 & DS3004, Data Warehousing & Business Intelligence, FAST-NUCES, Fall 2026

This project loads one retail transaction file into a MySQL star schema. Python implements the MESHJOIN algorithm, which joins the transactions with Master Data held on disk. Online sales arrive as a stream, store sales arrive in hourly groups, and 20 SQL queries then analyse the result.

**Which Live load this submission uses (PDF section 16):** the bonus cache, by default. Live join results go to the `live_cache` table first and are copied to the fact table in a slow period. To insert them straight into the fact table instead, start the program with `--live-load direct`. The program prints which one is active when it starts.

**Time scale (PDF section 5.3):** one simulated hour is 60 seconds of real time. `--seconds-per-hour` changes this, and the program prints the scale when it starts.

## 1. What you need

- Python 3.12
- MySQL Server 8.0 or later, running
- About 1 GB of free memory (data preparation peaked at 0.49 GB) and 1 GB of disk

The program needs the file `Online Retail.xlsx`. It is not in the zip because of its size, and the program offers to download it (see step 4).

Open a terminal in the folder that holds `Create-DW`, `Mesh-Join` and the others. All commands below start there.

## 2. Install

Windows (PowerShell):

```powershell
py -3.12 -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r Mesh-Join\requirements.txt
```

Linux or macOS:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r Mesh-Join/requirements.txt
```

## 3. Create the database

Make sure MySQL is running. On Windows, in an administrator PowerShell, if the service is stopped:

```powershell
Start-Service MySQL80
```

**You do not have to create anything by hand.** At startup the program creates the database you name, then runs `Create-DW/create_dw.sql`, which drops any old copy of these tables and the view `STORE_QUARTERLY_SALES` before creating them (PDF section 9).

To run the script yourself, for example to inspect the schema:

```bash
mysql -h localhost -P 3306 -u root -p -e "CREATE DATABASE IF NOT EXISTS retail_dw"
mysql -h localhost -P 3306 -u root -p retail_dw < Create-DW/create_dw.sql
```

`-p` makes the client ask for the password, so it never appears in your command history.

## 4. Get the data file

Download `Online Retail.xlsx` and put it in a new folder, `Data/raw/`:

> https://archive.ics.uci.edu/dataset/352/online+retail

If the file is missing when the program starts in a terminal, it asks whether to download it for you, and checks the SHA-256 of what it receives.

## 5. Prepare the data and start the program

```bash
cd Mesh-Join
python main.py
```

The program asks for these. Press Enter to accept a value in brackets.

| Question | Example |
|---|---|
| Host | `localhost` |
| Port | `3306` |
| Database name | `retail_dw` |
| User name | `root` |
| Password | hidden as you type, never stored |
| Path of the online file | the default `Data/online_sales_stream.csv` |
| Path of Online Retail.xlsx | the default `Data/raw/Online Retail.xlsx` |

Then, in the order of PDF section 9, it creates the tables, cleans the file, builds and loads the Master Data, writes the online file, loads the 48 periodic groups into the staging table, prints the settings, and starts the three threads. Preparation takes about two minutes.

**Your other databases are safe.** If the database you name holds tables with this project's names but was not created by this program, it stops and lists them, and asks you to type the database name to confirm. It never drops a database.

You should see, among other lines:

```text
Transactions before cleaning: 541,909
Transactions after cleaning: 397,884
Customer Master Data: 4,338 tuples, 9 partitions
Product Master Data: 3,665 tuples, 8 partitions
Periodic staging table: 119,144 transactions in 48 groups, smallest 2,482, largest 2,483
Peak time: 80 transactions per second
Non-peak time: 10 transactions per second
Stream arrival mode: peak
One simulated hour: 60 seconds
Stream buffer limit: 1,000 transactions
```

### Options

| Flag | Meaning | Default |
|---|---|---|
| `--peak-rate N` | online transactions per second at peak | 80 |
| `--non-peak-rate N` | online transactions per second at non-peak | 10 |
| `--seconds-per-hour S` | real seconds in one simulated hour | 60 |
| `--start-speed peak\|non-peak` | speed at startup | peak |
| `--start-mode online\|periodic\|both` | which forms run at startup | both |
| `--live-load cache\|direct` | how Live results enter the warehouse | cache |
| `--resume` | continue an interrupted run (section 8) | off |
| `--log-dir PATH` | where log files go | `Mesh-Join/logs` |

The MESHJOIN settings of PDF section 12 (partition 500, chunk 500, buffer 1,000, 48 groups) are fixed in the code on purpose.

## 6. Use the commands

Type these while the program runs. `status` shows that a switch worked.

| Command | Effect |
|---|---|
| `online` | Online form only. Periodic groups are not extracted. |
| `periodic` | Periodic groups only. The stream reader pauses. |
| `both` | Both forms. Live transactions are taken first. |
| `peak` | Fast speed. Shedding periodic transactions to disk is allowed. |
| `non-peak` | Slow speed. Processing shed transactions is allowed, and the Live cache empties. |
| `peak 120`, `non-peak 5` | Switch speed and also change that speed's rate. |
| `status` | Print the mode, speed, Stream buffer, Hash table H, Queue, Disk buffer and shed disk counts. |
| `help` | List the commands. |
| `quit` | Stop the threads, finish what is in memory, print the row accounting, and close. |

**To load everything:** leave it in `both`. Near the end it prints that both streams have finished, and asks you to type `non-peak`, so that the shed rows and the cache can finish. It then prints `ALL DATA LOADED`. At the official rates this took 77 minutes in the reference run, with a mix of peak and non-peak, because 278,740 online rows at 80 per second is 58 minutes.

**For a quick demonstration** at the same proportions, in about 9 minutes:

```bash
python main.py --seconds-per-hour 6 --peak-rate 800 --non-peak-rate 100
```

Expected at the end:

```text
In the fact table:   397,884
Balanced: every transaction is accounted for.
Fact quantity 5,167,812, revenue 8,911,407.904
Complete: every transaction has been through the whole ETL.
```

## 7. Run the queries

After the load, from the project folder:

```bash
mysql -h localhost -P 3306 -u root -p --table retail_dw < Queries-DW/queries_dw.sql
```

Add `> results.txt` to keep the output, because Q13, Q14, Q17, Q18 and Q19 return thousands of rows. The whole script took 73 seconds on a stock MySQL 8.0 server. Each query in the script has a banner with its question and interpretation. Question 20 creates the view `STORE_QUARTERLY_SALES`, then reads it in the required order.

## 8. Recover from an interruption

If the program or the machine stops mid-run, start it again with the same answers and `--resume`:

```bash
python main.py --resume
```

Tables are kept and preparation is skipped. Each transaction still ends up in the warehouse exactly once. This has been tested by killing the process mid-run (see `TESTING.md`). Without `--resume` the program rebuilds everything, as PDF section 9 requires.

## 9. Optional extras

These are **not part of the assignment**.

**Dashboard.** A separate, read-only web page over the warehouse, with a live view of a run, charts, and the 20 queries. It only listens on your own machine, and it asks for the same connection details.

```bash
cd Mesh-Join
python -m retail_dw.dashboard
```

Then open http://127.0.0.1:8765/ .

**A throwaway MySQL in Docker**, if you do not want to use your own server. It listens on port 3307 and keeps no data after it is removed.

```bash
cd Mesh-Join
cp .env.example .env     # then set your own MYSQL_ROOT_PASSWORD in .env
docker compose up -d --wait
```

Answer `127.0.0.1` and `3307` at the prompts. `docker compose down` removes it.

**Tests.** See `TESTING.md`.

## 10. Troubleshooting

| Message | What to do |
|---|---|
| `Could not reach MySQL` | The server is not running, or the host or port is wrong. On Windows see step 3. |
| `Access denied` | Wrong user name or password. On a terminal the program asks again. |
| `was not created by this program` | That database holds another project's tables. Use a different name, such as `retail_dw`. |
| `Raw data file not found` | Put the file in `Data/raw/`, or give its path at the prompt. |
| `has no finished data preparation to resume` | Preparation never finished in that database. Start without `--resume`. |
| `ERROR: the run is stopping` | The message gives the cause. Rows on disk are safe. Fix it and start with `--resume`. |

Logs are in `Mesh-Join/logs/`, with one summary line per simulated hour.

## 11. Interpretations of the assignment

The PDF leaves these open or says them in two ways. These are **my readings, not requirements of the PDF**.

| Point | Reading used |
|---|---|
| `processing_mode` (named in 6.2, never defined) | The route the row took: `Live`, `Periodic` or `Shed`. |
| Where the store is added (4.2 says the product pass, 5.4.3 says outside MESHJOIN) | Outside: from the in-memory `Store_list`, when the row is consolidated. |
| "Materialized view" (section 11) and "view" (Q20) | A plain view. MySQL has no materialized views, and 6.2 forbids storing totals in a second fact table. |
| "About six" product partitions | 8, counted from the table as 4.2 instructs: 3,665 products. |
| "The loaded year" | 2011, because 2010 holds only December. December 2010 and Q4 2010 are the base for the first growth figures. |
| Q1 and Q11, which read almost the same | Q1 ranks over the whole year and then drills into months. Q11 ranks inside each month. |
| How long a chunk stays, with 9 and 8 partitions | 9 iterations, the longer cycle. Seeing one product partition again changes nothing. |
| "Can't cope" (4.3) | At peak, periodic rows that do not fit the Stream buffer when their group is released. A group of about 2,480 cannot fit in 1,000 slots. |
| "Longest Description" | Trim edge spaces, then take the longest. The first in file order wins a tie (54 products tie). |
| Dividing into 48 groups | Equal counts of the sorted list. A shopping cart can span two groups, and all rows still load. |
| "Both can be changed while running" (section 12) | `peak` and `non-peak` switch speed, and also accept a new rate. |
| Shed rows in `online` mode | Stay on disk, since that mode is "online form only". |
| "Slow period" for the bonus | Non-peak speed, plus a final flush at `quit`. |
| Q7 "average purchase amount" | Per transaction tuple, with a per-cart average column as well. |
| Q19 "average daily revenue" | Over the days the product actually sold. |
| Age group labels | ASCII hyphens, such as `0-17`. |

## 12. Dataset citation

Chen, D., Sain, S. L., and Guo, K. *Online Retail*. UCI Machine Learning Repository. https://archive.ics.uci.edu/dataset/352/online+retail . Downloaded 3 November 2026.
