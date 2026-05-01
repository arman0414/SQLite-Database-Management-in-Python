"""INNER / LEFT JOIN between two CSV tables."""
from __future__ import annotations

import re
from typing import Any, Dict, List

from engine.parser import normalize_ws
from engine.sql_util import apply_where, orderby_rows, split_trailing_clauses
from storage.csv_backend import CsvBackend
from storage.schema_store import SchemaStore


def run_join(stmt: str, backend: CsvBackend, schema: SchemaStore) -> List[Dict[str, Any]]:
    s = normalize_ws(stmt)
    base, where_extra, order_expr, limit_n = split_trailing_clauses(s)

    m = re.match(
        r"SELECT\s+(?P<sel>.+?)\s+FROM\s+(?P<a>\w+)\s+(?P<kind>INNER|LEFT)\s+JOIN\s+(?P<b>\w+)\s+ON\s+(?P<on>.+)$",
        base,
        flags=re.I,
    )
    if not m:
        raise ValueError("JOIN parse error")

    sel = m.group("sel").strip()
    ta = m.group("a").lower()
    tb = m.group("b").lower()
    kind = m.group("kind").upper()
    on_clause = m.group("on").strip()

    onm = re.match(r"(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)", on_clause, flags=re.I)
    if not onm:
        raise ValueError("ON clause must be a.x = b.y")
    _a_alias, a_col, _b_alias, b_col = onm.groups()
    a_col = a_col.lower()
    b_col = b_col.lower()

    rows_a = backend.read_rows(ta)
    rows_b = backend.read_rows(tb)

    out: List[Dict[str, Any]] = []
    for ra in rows_a:
        matched = False
        ra_l = {k.lower(): v for k, v in ra.items()}
        for rb in rows_b:
            rb_l = {k.lower(): v for k, v in rb.items()}
            if str(ra_l.get(a_col)) == str(rb_l.get(b_col)):
                matched = True
                merged: Dict[str, Any] = {}
                for k, v in ra_l.items():
                    merged[f"{ta}.{k}"] = v
                for k, v in rb_l.items():
                    merged[f"{tb}.{k}"] = v
                if where_extra and not apply_where(merged, where_extra):
                    continue
                out.append(_project(sel, merged))
        if kind == "LEFT" and not matched:
            merged = {f"{ta}.{k}": v for k, v in ra_l.items()}
            for k in schema.get_columns(tb):
                merged[f"{tb}.{k}"] = None
            if where_extra and not apply_where(merged, where_extra):
                continue
            out.append(_project(sel, merged))

    if order_expr:
        om = re.match(r"([\w.]+)(?:\s+(ASC|DESC))?$", order_expr, flags=re.I)
        if om:
            col = om.group(1).lower()
            direction = (om.group(2) or "ASC").lower()
            out = orderby_rows(out, f"{col} {direction}")
    if limit_n:
        out = out[: int(limit_n)]
    return out


def _project(sel: str, row: Dict[str, Any]) -> Dict[str, Any]:
    if sel.strip() == "*":
        return dict(row)
    cols = [c.strip().lower() for c in sel.split(",")]
    return {c: row.get(c) for c in cols}
