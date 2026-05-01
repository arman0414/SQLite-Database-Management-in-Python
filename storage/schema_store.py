"""Persist table schemas as JSON for CSV mode."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List


class SchemaStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.path.write_text("{}", encoding="utf-8")

    def _load(self) -> Dict[str, List[str]]:
        return json.loads(self.path.read_text(encoding="utf-8"))

    def _save(self, data: Dict[str, List[str]]) -> None:
        self.path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    def get_columns(self, table: str) -> List[str]:
        data = self._load()
        return list(data.get(table, []))

    def set_columns(self, table: str, columns: List[str]) -> None:
        data = self._load()
        data[table] = columns
        self._save(data)

    def drop_table(self, table: str) -> None:
        data = self._load()
        data.pop(table, None)
        self._save(data)

    def list_tables(self) -> List[str]:
        return sorted(self._load().keys())
