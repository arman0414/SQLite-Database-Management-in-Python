import tempfile
import unittest
from pathlib import Path

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from engine.ddl_engine import execute_ddl_csv
from storage.csv_backend import CsvBackend
from storage.schema_store import SchemaStore


class DdlTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.backend = CsvBackend(self.tmp)
        self.schema = SchemaStore(self.tmp / "schema_store.json")

    def test_create_drop_alter(self):
        msg = execute_ddl_csv(
            "CREATE TABLE demo (id INT, name TEXT)", self.backend, self.schema
        )
        self.assertIn("created", msg.lower())
        self.assertEqual(self.schema.get_columns("demo"), ["id", "name"])

        execute_ddl_csv("ALTER TABLE demo ADD COLUMN age INT", self.backend, self.schema)
        self.assertIn("age", self.schema.get_columns("demo"))

        execute_ddl_csv("DROP TABLE demo", self.backend, self.schema)
        self.assertEqual(self.schema.get_columns("demo"), [])


if __name__ == "__main__":
    unittest.main()
