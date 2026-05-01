"""SELECT with WHERE, ORDER BY, LIMIT (single-table and simple joins)."""
from __future__ import annotations

import re
from typing import Any, Dict, List

from engine.aggregator import apply_aggregates
from engine.join_handler import run_join
from engine.parser import normalize_ws
from engine.sql_util import apply_where, orderby_rows, split_trailing_clauses
from storage.csv_backend import CsvBackend
from storage.schema_store import SchemaStore


def execute_select_csv(stmt: str, backend: CsvBackend, schema: SchemaStore) -> List[Dict[str, Any]]:
    s = normalize_ws(stmt)
    if re.search(r"\bGROUP\s+BY\b", s, flags=re.I):
        return apply_aggregates(s, backend, schema)
    if re.search(r"\bJOIN\b", s, flags=re.I):
        return run_join(s, backend, schema)

    core, where_part, order_expr, limit_n = split_trailing_clauses(s)

    m = re.match(r"SELECT\s+(?P<cols>[\w*,\s.]+)\s+FROM\s+(?P<table>\w+)$", core, flags=re.I)
    if not m:
        raise ValueError("SELECT parse error")

    cols_raw = m.group("cols").replace(" ", "")
    table = m.group("table").lower()

    rows = [{k.lower(): v for k, v in r.items()} for r in backend.read_rows(table)]
    if where_part:
        rows = [r for r in rows if apply_where(r, where_part)]

    if cols_raw != "*":
        wanted = [c.strip().lower() for c in cols_raw.split(",")]
        rows = [{c: r.get(c) for c in wanted} for r in rows]

    if order_expr:
        om = re.match(r"([\w.]+)(?:\s+(ASC|DESC))?$", order_expr, flags=re.I)
        if om:
            col = om.group(1).lower()
            direction = (om.group(2) or "ASC").lower()
            rows = orderby_rows(rows, f"{col} {direction}")
    if limit_n:
        rows = rows[: int(limit_n)]
    return rows
