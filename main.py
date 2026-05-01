#!/usr/bin/env python3
"""CLI entry: CSV mini-engine vs passthrough SQLite mode."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.ddl_engine import execute_ddl_csv
from engine.dml_engine import execute_delete, execute_insert, execute_update
from engine.parser import normalize_ws, split_statements
from engine.query_engine import execute_select_csv
from storage.csv_backend import CsvBackend
from storage.schema_store import SchemaStore
from storage.sqlite_backend import SqliteBackend
from cli.formatter import format_rows


def _dispatch_csv(stmt: str, backend: CsvBackend, schema: SchemaStore):
    s = normalize_ws(stmt).upper()
    if s.startswith("CREATE") or s.startswith("DROP") or s.startswith("ALTER"):
        return execute_ddl_csv(stmt, backend, schema)
    if s.startswith("INSERT"):
        return execute_insert(stmt, backend, schema)
    if s.startswith("UPDATE"):
        return execute_update(stmt, backend, schema)
    if s.startswith("DELETE"):
        return execute_delete(stmt, backend, schema)
    if s.startswith("SELECT"):
        return execute_select_csv(stmt, backend, schema)
    raise ValueError("Unsupported statement for CSV mode")


def main() -> None:
    ap = argparse.ArgumentParser(description="Mini SQL engine")
    ap.add_argument("--mode", choices=["csv", "sqlite"], required=True)
    ap.add_argument("--data-dir", type=Path, default=ROOT / "data")
    ap.add_argument("--sqlite-path", type=Path, default=ROOT / "engine.db")
    ap.add_argument("--repl", action="store_true", help="Start interactive shell")
    ap.add_argument("--sql", type=str, help="Execute one statement from CLI")
    ap.add_argument(
        "--sql-file",
        type=Path,
        help="Run multiple statements from a .sql file (semicolon-separated)",
    )
    args = ap.parse_args()

    if args.mode == "sqlite":
        be = SqliteBackend(args.sqlite_path)

        def executor(stmt: str):
            st = stmt.strip().rstrip(";")
            if st.upper().startswith("SELECT"):
                rows = be.fetchall(st)
                return [dict(r) for r in rows]
            be.execute(st)
            return "OK"

        def run_stmts(stmts: list[str]) -> None:
            for raw in stmts:
                st = raw.strip()
                if not st:
                    continue
                out = executor(st.rstrip(";"))
                if isinstance(out, list):
                    sys.stdout.write(format_rows(out))
                else:
                    print(out)

        if args.repl:
            from cli.repl import start_repl

            start_repl(executor)
        elif args.sql_file:
            run_stmts(split_statements(args.sql_file.read_text(encoding="utf-8")))
        elif args.sql:
            out = executor(args.sql.rstrip(";"))
            if isinstance(out, list):
                sys.stdout.write(format_rows(out))
            else:
                print(out)
        else:
            ap.error("Provide --repl, --sql, or --sql-file for sqlite mode")
        be.close()
        return

    backend = CsvBackend(args.data_dir)
    schema = SchemaStore(args.data_dir / "schema_store.json")

    def executor_csv(stmt: str):
        return _dispatch_csv(stmt, backend, schema)

    def run_csv_stmts(stmts: list[str]) -> None:
        for raw in stmts:
            st = raw.strip()
            if not st:
                continue
            out = executor_csv(st.rstrip(";"))
            if isinstance(out, list):
                sys.stdout.write(format_rows(out))
            else:
                print(out)

    if args.repl:
        from cli.repl import start_repl

        start_repl(executor_csv)
    elif args.sql_file:
        run_csv_stmts(split_statements(args.sql_file.read_text(encoding="utf-8")))
    elif args.sql:
        out = executor_csv(args.sql.rstrip(";"))
        if isinstance(out, list):
            sys.stdout.write(format_rows(out))
        else:
            print(out)
    else:
        ap.error("Provide --repl, --sql, or --sql-file for csv mode")


if __name__ == "__main__":
    main()
