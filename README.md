# SQLite Database Management Engine

Educational dual-mode SQL tool:

- **CSV mode** — lightweight parser + execution engine storing each logical table as a CSV file plus a JSON schema catalog.
- **SQLite mode** — passes statements directly to Python’s built-in `sqlite3` module while reusing the same CLI/REPL UX.

## Architecture

```
cli/repl.py ──► main.py dispatcher
                 ├─ csv: parser + ddl/dml/query + join + aggregate
                 └─ sqlite: storage/sqlite_backend.py (thin wrapper)

CSV stack:
  engine/parser.py ─► ddl_engine / dml_engine / query_engine
                    └► join_handler / aggregator
                       └► storage/csv_backend.py + schema_store.json
```

## Usage
Multi-statement scripts (comments + `;` splitting) via `parser.split_statements`:

```bash
python main.py --mode csv --data-dir ./data --sql-file sample_queries.sql
```


```bash
# Interactive CSV engine (default data in ./data)
python main.py --mode csv --repl

# One-shot CSV query
python main.py --mode csv --sql "CREATE TABLE demo (id INT);"
python main.py --mode csv --data-dir ./data --sql "SELECT * FROM demo;"

# SQLite passthrough (loads real SQL)
python main.py --mode sqlite --sqlite-path ./engine.db --sql "SELECT sqlite_version();"
```

See `sample_queries.sql` for examples. Run the whole file with `--sql-file` (CSV or SQLite mode); each statement is split on `;` outside of quoted strings.

## Supported SQL (CSV mode)

| Area | Syntax highlights |
|------|-------------------|
| DDL | `CREATE TABLE`, `DROP TABLE`, `ALTER TABLE ADD COLUMN` |
| DML | `INSERT`, `UPDATE ... SET ... WHERE`, `DELETE ... WHERE` |
| Query | `SELECT` projections, `WHERE` (`AND`, comparisons), `ORDER BY`, `LIMIT` |
| Join | `INNER JOIN` / `LEFT JOIN` … `ON a.col = b.col` |
| Aggregate | `COUNT(*)`, `SUM`, `AVG`, `MIN`, `MAX` with `GROUP BY` |

Identifiers are normalized to lowercase; CSV cells are auto-coerced to numbers when possible.

## Tests

```bash
python3 tests/test_runner.py
# or
python3 -m unittest discover -s tests -p 'test_*.py' -v
```

## CSV vs SQLite mode

| | CSV mode | SQLite mode |
|---|----------|---------------|
| Storage | `*.csv` + `schema_store.json` | Single `.db` file |
| SQL surface | Curated subset above | Full SQLite |
| Purpose | Demonstrate parsing + custom engine | Production-grade queries |

## Limitations

- CSV engine SQL coverage is intentionally small (no subqueries, no HAVING).
- `ALTER TABLE` supports `ADD COLUMN` only.
- `UPDATE`/`DELETE` WHERE clauses use the same simple predicate grammar as `SELECT`.
