"""CSV-backed table storage."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Dict, List, Optional


class CsvBackend:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def table_path(self, name: str) -> Path:
        safe = name.lower()
        return self.data_dir / f"{safe}.csv"

    def read_rows(self, name: str) -> List[Dict[str, Any]]:
        path = self.table_path(name)
        if not path.exists():
            return []
        with path.open(newline="", encoding="utf-8") as f:
            rows = []
            for raw in csv.DictReader(f):
                row: Dict[str, Any] = {}
                for k, v in raw.items():
                    key = (k or "").lower()
                    if v is None or v == "":
                        row[key] = ""
                        continue
                    try:
                        row[key] = int(v)
                    except ValueError:
                        try:
                            row[key] = float(v)
                        except ValueError:
                            row[key] = v
                rows.append(row)
            return rows

    def write_rows(self, name: str, columns: List[str], rows: List[Dict[str, Any]]) -> None:
        path = self.table_path(name)
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
            w.writeheader()
            for r in rows:
                w.writerow({c: r.get(c, "") for c in columns})

    def drop_file(self, name: str) -> None:
        p = self.table_path(name)
        if p.exists():
            p.unlink()
