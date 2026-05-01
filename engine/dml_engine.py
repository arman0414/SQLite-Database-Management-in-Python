"""INSERT / UPDATE / DELETE for CSV mode."""
from __future__ import annotations

import csv
import io
import re
from typing import Any, Dict, List, Tuple

from engine.parser import normalize_ws
from engine.sql_util import apply_where
from storage.csv_backend import CsvBackend
from storage.schema_store import SchemaStore


def _parse_insert(stmt: str) -> Tuple[str, List[str], List[str]]:
    s = normalize_ws(stmt)
    m = re.match(
        r"INSERT\s+INTO\s+(\w+)\s*(?:\(([^)]+)\))?\s*VALUES\s*\(([^)]+)\)",
        s,
        flags=re.I,
    )
    if not m:
        raise ValueError("INSERT parse error")
    table = m.group(1).lower()
    cols_part = m.group(2)
    vals_part = m.group(3)
    if cols_part:
        cols = [c.strip().lower() for c in cols_part.split(",")]
    else:
        cols = []
    reader = csv.reader(io.StringIO(vals_part), skipinitialspace=True)
    vals = next(reader)
    return table, cols, vals


def _coerce(val: str) -> Any:
    v = val.strip()
    if (v.startswith("'") and v.endswith("'")) or (v.startswith('"') and v.endswith('"')):
        return v[1:-1]
    try:
        if "." in v:
            return float(v)
        return int(v)
    except ValueError:
        return v


def execute_insert(stmt: str, backend: CsvBackend, schema: SchemaStore) -> str:
    table, cols, vals = _parse_insert(stmt)
    row_cols = schema.get_columns(table)
    if not row_cols:
        raise ValueError(f"Unknown table {table}")
    if not cols:
        cols = row_cols
    if len(cols) != len(vals):
        raise ValueError("Column/value mismatch")
    values = [_coerce(v) for v in vals]
    row = dict(zip(cols, values))
    rows = [{k.lower(): v for k, v in r.items()} for r in backend.read_rows(table)]
    full = {c: row.get(c, "") for c in row_cols}
    rows.append(full)
    backend.write_rows(table, row_cols, rows)
    return "OK 1 row inserted"


def execute_update(stmt: str, backend: CsvBackend, schema: SchemaStore) -> str:
    s = normalize_ws(stmt)
    m = re.match(
        r"UPDATE\s+(\w+)\s+SET\s+(\w+)\s*=\s*([^ ]+)\s+WHERE\s+(.+)$",
        s,
        flags=re.I,
    )
    if not m:
        raise ValueError("UPDATE parse error (simple SET col=val WHERE supported)")
    table = m.group(1).lower()
    col = m.group(2).lower()
    val = _coerce(m.group(3))
    where = m.group(4)
    row_cols = schema.get_columns(table)
    rows = [{k.lower(): v for k, v in r.items()} for r in backend.read_rows(table)]
    new_rows: List[Dict[str, Any]] = []
    changed = 0
    for r in rows:
        if apply_where(r, where):
            r = dict(r)
            r[col] = val
            changed += 1
        new_rows.append(r)
    backend.write_rows(table, row_cols, new_rows)
    return f"OK {changed} rows updated"


def execute_delete(stmt: str, backend: CsvBackend, schema: SchemaStore) -> str:
    s = normalize_ws(stmt)
    m = re.match(r"DELETE\s+FROM\s+(\w+)\s+WHERE\s+(.+)$", s, flags=re.I)
    if not m:
        raise ValueError("DELETE parse error")
    table = m.group(1).lower()
    where = m.group(2)
    row_cols = schema.get_columns(table)
    rows = [{k.lower(): v for k, v in r.items()} for r in backend.read_rows(table)]
    kept = [r for r in rows if not apply_where(r, where)]
    removed = len(rows) - len(kept)
    backend.write_rows(table, row_cols, kept)
    return f"OK {removed} rows deleted"
