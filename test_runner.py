import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def main():
    tests = [
        ("crud", "sql/pa2_test.sql"),
        ("joins", "sql/pa3_test.sql"),
    ]
    bad = 0
    for mode, sql in tests:
        cmd = [sys.executable, "main.py", mode, "--sql", sql]
        r = subprocess.run(cmd, cwd=ROOT)
        label = "ok" if r.returncode == 0 else "fail"
        print("[" + label + "] " + mode + " " + sql)
        if r.returncode != 0:
            bad += 1

    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
