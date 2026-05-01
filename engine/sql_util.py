"""WHERE evaluation and sorting helpers."""
from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

from engine.parser import normalize_ws


def _split_and(where: str) -> List[str]:
    return [p.strip() for p in re.split(r"\s+AND\s+", where, flags=re.I) if p.strip()]


def _cmp(a: Any, b: Any, op: str) -> bool:
    if op == "=":
        return a == b
    if op in ("!=", "<>"):
        return a != b
    if op == ">":
        return a > b
    if op == "<":
        return a < b
    if op == ">=":
        return a >= b
    if op == "<=":
        return a <= b
    raise ValueError(f"unknown op {op}")


def coerce_atom(raw: str) -> Any:
    raw = raw.strip()
    if (raw.startswith("'") and raw.endswith("'")) or (raw.startswith('"') and raw.endswith('"')):
        return raw[1:-1]
    try:
        return int(raw)
    except ValueError:
        try:
            return float(raw)
        except ValueError:
            return raw


def apply_where(row: Dict[str, Any], where: str) -> bool:
    if not where.strip():
        return True
    for clause in _split_and(where):
        m = re.match(r"([\w.]+)\s*(=|!=|<>|>=|<=|>|<)\s*(.+)$", clause)
        if not m:
            raise ValueError(f"Bad WHERE clause: {clause}")
        col, op, rhs = m.group(1).lower(), m.group(2), m.group(3).strip()
        val = coerce_atom(rhs)
        if not _cmp(row.get(col), val, op):
            return False
    return True


def split_trailing_clauses(stmt: str) -> Tuple[str, str | None, str | None, str | None]:
    """Strip ORDER BY / LIMIT / WHERE from the end; return core SQL first."""
    s = normalize_ws(stmt)
    where = None
    order = None
    limit = None

    lim_m = re.search(r"\bLIMIT\s+(\d+)\s*$", s, flags=re.I)
    if lim_m:
        limit = lim_m.group(1)
        s = s[: lim_m.start()].strip()

    ord_m = re.search(r"\bORDER\s+BY\s+(.+)$", s, flags=re.I)
    if ord_m:
        order = ord_m.group(1).strip()
        s = s[: ord_m.start()].strip()

    wh_m = re.search(r"\bWHERE\s+(.+)$", s, flags=re.I)
    if wh_m:
        where = wh_m.group(1).strip()
        s = s[: wh_m.start()].strip()

    return s, where, order, limit


def orderby_rows(rows: List[Dict[str, Any]], order: str) -> List[Dict[str, Any]]:
    col, direction = order.split()
    rev = direction == "desc"

    def key(r: Dict[str, Any]):
        v = r.get(col)
        try:
            return float(v)
        except Exception:
            return v

    return sorted(rows, key=key, reverse=rev)
