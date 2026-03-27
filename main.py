import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent

MODES = {
    "base": ROOT / "src/engine_base.py",
    "crud": ROOT / "src/engine_crud.py",
    "joins": ROOT / "src/engine_joins.py",
    "locks": ROOT / "src/engine_locks.py",
    "sqlite": ROOT / "src/sqlite_aggregates.py",
}

ALIASES = {
    "pa1": "base",
    "pa2": "crud",
    "pa3": "joins",
    "pa4": "locks",
    "pa5": "sqlite",
}


def resolve_mode(name):
    key = name.lower()
    if key in ALIASES:
        key = ALIASES[key]
    return key


def run_mode(mode, sql_file=None):
    key = resolve_mode(mode)
    path = MODES.get(key)
    if path is None or not path.exists():
        print("Missing script for mode:", mode)
        return 1

    if sql_file is None or key == "sqlite":
        p = subprocess.run([sys.executable, str(path)], cwd=ROOT)
        return p.returncode

    full = ROOT / sql_file
    if not full.is_file():
        print("Can't find SQL file:", sql_file)
        return 1

    text = full.read_text(encoding="utf-8", errors="ignore").replace("\r", "")
    p = subprocess.run(
        [sys.executable, str(path)],
        cwd=ROOT,
        input=text,
        text=True,
    )
    return p.returncode


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "demo":
        import demo

        return demo.main()

    choices = sorted(set(MODES.keys()) | set(ALIASES.keys()))
    parser = argparse.ArgumentParser(
        description="Mini database — pick a mode (pa1–pa5 still work as shortcuts).",
    )
    parser.add_argument(
        "mode",
        choices=choices,
        help="base | crud | joins | locks | sqlite  (aliases: pa1–pa5)",
    )
    parser.add_argument("--sql", default=None)
    if len(sys.argv) == 1:
        parser.print_help()
        return 0
    args = parser.parse_args()
    return run_mode(args.mode, args.sql)


if __name__ == "__main__":
    sys.exit(main())
