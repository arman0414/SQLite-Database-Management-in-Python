"""GROUP BY with COUNT/SUM/AVG/MIN/MAX."""
from __future__ import annotations

import re
from collections import defaultdict
from statistics import mean
from typing import Any, Dict, List

from engine.parser import normalize_ws
from engine.sql_util import apply_where
from storage.csv_backend import CsvBackend
from storage.schema_store import SchemaStore


def _agg_funcs(part: str) -> tuple[str, str]:
    m = re.match(r"(COUNT)\(\*\)", part, flags=re.I)
    if m:
        return "count_star", "*"
    m = re.match(r"(SUM|AVG|MIN|MAX)\(\s*(\w+)\s*\)", part, flags=re.I)
    if not m:
        raise ValueError(f"Unsupported aggregate {part}")
    return m.group(1).lower(), m.group(2).lower()


def apply_aggregates(stmt: str, backend: CsvBackend, schema: SchemaStore) -> List[Dict[str, Any]]:
    s = normalize_ws(stmt)
    if "GROUP BY" not in s.upper():
        raise ValueError("GROUP BY missing")

    head, gb_part = re.split(r"\bGROUP\s+BY\b", s, maxsplit=1, flags=re.I)
    gb_col = gb_part.strip().split()[0].lower()

    where = None
    if re.search(r"\bWHERE\b", head, flags=re.I):
        front, where = re.split(r"\bWHERE\b", head, maxsplit=1, flags=re.I)
        head = front
        where = where.strip()

    m = re.match(r"SELECT\s+(?P<select>.+?)\s+FROM\s+(?P<table>\w+)", head, flags=re.I)
    if not m:
        raise ValueError("GROUP BY SELECT parse error")

    select_part = m.group("select")
    table = m.group("table").lower()

    rows = [{k.lower(): v for k, v in r.items()} for r in backend.read_rows(table)]
    if where:
        rows = [r for r in rows if apply_where(r, where)]

    buckets: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        buckets[r.get(gb_col)].append(r)

    items = [x.strip() for x in select_part.split(",")]
    out: List[Dict[str, Any]] = []
    for key, group in buckets.items():
        row_out: Dict[str, Any] = {}
        for item in items:
            low = item.lower()
            if low == gb_col:
                row_out[gb_col] = key
                continue
            fn, col = _agg_funcs(item)
            if fn == "count_star":
                row_out["count(*)"] = len(group)
            elif fn == "sum":
                row_out[f"sum({col})"] = sum(float(r[col]) for r in group)
            elif fn == "avg":
                row_out[f"avg({col})"] = round(mean(float(r[col]) for r in group), 4)
            elif fn == "min":
                row_out[f"min({col})"] = min(float(r[col]) for r in group)
            elif fn == "max":
                row_out[f"max({col})"] = max(float(r[col]) for r in group)
        out.append(row_out)
    return out
