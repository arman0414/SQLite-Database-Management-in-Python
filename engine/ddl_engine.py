"""CREATE / DROP / ALTER for CSV-backed engine."""
from __future__ import annotations

import re
from typing import List

from engine.parser import normalize_ws
from storage.csv_backend import CsvBackend
from storage.schema_store import SchemaStore


def _parse_create(stmt: str) -> tuple[str, List[str]]:
    m = re.match(
        r"CREATE\s+TABLE\s+(?P<name>\w+)\s*\((?P<cols>[^)]+)\)\s*;?$",
        normalize_ws(stmt),
        flags=re.I,
    )
    if not m:
        raise ValueError("Unsupported CREATE TABLE syntax")
    raw_cols = m.group("cols")
    cols = [c.strip().split()[0] for c in raw_cols.split(",") if c.strip()]
    return m.group("name").lower(), cols


def execute_ddl_csv(stmt: str, backend: CsvBackend, schema: SchemaStore) -> str:
    s = normalize_ws(stmt)
    if s.upper().startswith("CREATE TABLE"):
        name, cols = _parse_create(stmt)
        schema.set_columns(name, cols)
        backend.write_rows(name, cols, [])
        return f"OK created table {name}"
    if s.upper().startswith("DROP TABLE"):
        m = re.match(r"DROP\s+TABLE\s+(?P<name>\w+)", s, flags=re.I)
        if not m:
            raise ValueError("DROP parse error")
        name = m.group("name").lower()
        schema.drop_table(name)
        backend.drop_file(name)
        return f"OK dropped {name}"
    if s.upper().startswith("ALTER TABLE"):
        m = re.match(
            r"ALTER\s+TABLE\s+(?P<t>\w+)\s+ADD\s+COLUMN\s+(?P<c>\w+)\s+(?P<typ>\w+)",
            s,
            flags=re.I,
        )
        if not m:
            raise ValueError("Only ALTER TABLE ADD COLUMN is supported")
        t = m.group("t").lower()
        c = m.group("c").lower()
        cols = schema.get_columns(t)
        if not cols:
            raise ValueError(f"Unknown table {t}")
        cols = cols + [c]
        rows = backend.read_rows(t)
        for r in rows:
            r[c] = ""
        schema.set_columns(t, cols)
        backend.write_rows(t, cols, rows)
        return f"OK altered {t} add {c}"
    raise ValueError("DDL not recognized")
