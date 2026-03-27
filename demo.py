import shutil
import subprocess
import sys
from pathlib import Path

here = Path(__file__).resolve().parent
sql = "sql/demo_scenario.sql"


def main():
    demo_dir = here / "DemoDB"
    if demo_dir.exists():
        shutil.rmtree(demo_dir, ignore_errors=True)

    db = here / "db_tpch.db"
    if db.exists():
        db.unlink()

    entry = str(here / "main.py")
    py = sys.executable

    print("--- crud mode (mini engine) ---")
    sys.stdout.flush()
    a = subprocess.run(
        [py, entry, "crud", "--sql", sql],
        cwd=str(here),
        text=True,
    )

    print()
    print("--- sqlite mode ---")
    sys.stdout.flush()
    b = subprocess.run([py, entry, "sqlite"], cwd=str(here), text=True)

    print()
    print("Done.")
    print("  crud: DemoDB + Item table + 2 rows + select")
    print("  sqlite: db_tpch.db, Part table, count/avg/max")

    if a.returncode != 0 or b.returncode != 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
