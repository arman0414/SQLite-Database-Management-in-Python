# SQLite Database Management in Python

Mini database project in Python: custom SQL-like modes, SQLite, tests, and a demo.

## Quick steps

1. **Go to the project folder** (where `main.py` is):  
   `cd path/to/SQLite-Database-Management-in-Python`

2. **See options:** `python main.py --help`  
   Modes: `base`, `crud`, `joins`, `locks`, `sqlite` (aliases `pa1`–`pa5`).

3. **Run a mode interactively:** `python main.py crud`  
   (Use `.exit` when done if the prompt expects it.)

4. **Run with a SQL file:**  
   `python main.py crud --sql sql/pa2_test.sql`  
   `python main.py joins --sql sql/pa3_test.sql`

5. **Demo (crud + sqlite):** `python demo.py` or `python main.py demo`

6. **Tests:** `python test_runner.py`

---

## Modes

| Mode | Role |
|------|------|
| `base` | Core DB + tables |
| `crud` | CRUD + file-backed tables |
| `joins` | Join-style queries |
| `locks` | Locks / transaction-style flow |
| `sqlite` | SQLite file + aggregates |

Aliases: `pa1`→`base`, `pa2`→`crud`, `pa3`→`joins`, `pa4`→`locks`, `pa5`→`sqlite`.

## Layout

`src/` — engines · `sql/` — SQL inputs (tests + demo) · `data/` (scratch, gitignored)

## What each file is

| File | Purpose |
|------|---------|
| `main.py` | CLI: runs a mode (`crud`, `joins`, `sqlite`, …); optional `--sql file.sql` pipes SQL to stdin. |
| `demo.py` | One-shot demo: clears `DemoDB/` and `db_tpch.db`, runs CRUD on `sql/demo_scenario.sql`, then SQLite aggregates. |
| `test_runner.py` | Runs `sql/pa2_test.sql` and `sql/pa3_test.sql` through `main.py` and reports pass/fail. |
| `src/engine_base.py` | Mini engine: create/use DB, tables, basic ops (PA1-style). |
| `src/engine_crud.py` | CRUD engine: CSV-backed tables, `SELECT`/`UPDATE`/`DELETE`, multi-line SQL. |
| `src/engine_joins.py` | Join engine: comma join + `WHERE`, `INNER JOIN`, `LEFT OUTER JOIN`. |
| `src/engine_locks.py` | Locks / two-phase style flow demo. |
| `src/sqlite_aggregates.py` | SQLite script: builds or uses `db_tpch.db`, runs aggregate queries on `Part`. |
| `sql/pa2_test.sql` | Automated test input for the CRUD engine. |
| `sql/pa3_test.sql` | Automated test input for the join engine. |
| `sql/demo_scenario.sql` | SQL used by `demo.py` for the mini-engine half of the demo. |
| `data/.gitkeep` | Keeps the `data/` folder in the repo; real data files are ignored. |
| `.gitignore` | Ignores caches, generated DB folders, `DemoDB/`, `db_tpch.db`, and scratch `data/*`. |

## Tech

Python · `sqlite3` · on-disk files for custom modes
