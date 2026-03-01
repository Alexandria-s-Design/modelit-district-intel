#!/usr/bin/env python3
"""Get the next batch of unresearched districts by priority score."""

import argparse
import json
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parent.parent
DATA_FILE = REPO_DIR / "data" / "cde-districts.json"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--status", default="unresearched")
    args = parser.parse_args()

    with open(DATA_FILE) as f:
        districts = json.load(f)

    # Filter by status, already sorted by priority (descending)
    candidates = [d for d in districts if d["status"] == args.status]

    # Output district names (one per line) for shell consumption
    for d in candidates[:args.count]:
        print(d["name"])


if __name__ == "__main__":
    main()
