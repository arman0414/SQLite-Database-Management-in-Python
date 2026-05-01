#!/usr/bin/env python3
"""Run all tests with PASS/FAIL coloring."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"


def main() -> None:
    suite = unittest.defaultTestLoader.discover(str(Path(__file__).parent), pattern="test_*.py")
    runner = unittest.TextTestRunner(stream=sys.stdout, verbosity=1)
    result = runner.run(suite)
    print("\nSummary")
    for case, trace in result.failures + result.errors:
        print(FAIL, case)
    if result.wasSuccessful():
        print(PASS, f"ran={result.testsRun}")
    else:
        print(FAIL, f"failures={len(result.failures)} errors={len(result.errors)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
