import tempfile
import unittest
from pathlib import Path

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from engine.ddl_engine import execute_ddl_csv
from engine.dml_engine import execute_delete, execute_insert, execute_update
from engine.query_engine import execute_select_csv
from storage.csv_backend import CsvBackend
from storage.schema_store import SchemaStore


class DmlTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.backend = CsvBackend(self.tmp)
        self.schema = SchemaStore(self.tmp / "schema_store.json")
        execute_ddl_csv("CREATE TABLE items (id INT, name TEXT)", self.backend, self.schema)

    def test_insert_update_delete(self):
        execute_insert("INSERT INTO items (id,name) VALUES (1,'a')", self.backend, self.schema)
        rows = execute_select_csv("SELECT * FROM items", self.backend, self.schema)
        self.assertEqual(len(rows), 1)

        execute_update("UPDATE items SET name='b' WHERE id=1", self.backend, self.schema)
        rows = execute_select_csv("SELECT name FROM items WHERE id=1", self.backend, self.schema)
        self.assertEqual(rows[0]["name"], "b")

        execute_delete("DELETE FROM items WHERE id=1", self.backend, self.schema)
        rows = execute_select_csv("SELECT * FROM items", self.backend, self.schema)
        self.assertEqual(len(rows), 0)


if __name__ == "__main__":
    unittest.main()
