"""Extract the JSON measurements emitted by the opt-in Go experiment tests."""

import argparse
import csv
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite", choices=("hybrid", "scale"))
    parser.add_argument("log", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    marker = {"hybrid": "HYBRID_RESULT ", "scale": "SCALE_RESULT "}[args.suite]
    rows = [
        json.loads(line.split(marker, 1)[1])
        for line in args.log.read_text().splitlines()
        if marker in line
    ]
    if not rows:
        parser.error("no completed measurements in the supplied Go test log")
    with args.output.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
