#!/usr/bin/env python3
"""Build a throwaway test binary and run the fixed Following diagnostic matrix."""

import argparse
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile

BASE = "e9f1e4f82654951100250f617ac7b290bc92738f"
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
CELLS = [(k, variant, repeat) for k in (2, 50, 500)
         for repeat, order in ((1, ("current", "inbox-only")), (2, ("inbox-only", "current")))
         for variant in order]


def replace(path, before, after):
    content = path.read_text()
    if content.count(before) != 1:
        raise ValueError(f"diagnostic injection anchor must occur once in {path}")
    path.write_text(content.replace(before, after))


def prepare(work):
    archive = subprocess.check_output(["git", "archive", BASE, "backend"], cwd=ROOT)
    with tarfile.open(fileobj=io.BytesIO(archive)) as source:
        source.extractall(work, filter="data")
    backend = work / "backend"
    feed = backend / "internal/feed"
    (feed / "push_diagnostic_test.go").write_bytes((HERE / "push_diagnostic_test.go").read_bytes())
    (feed / "push_diagnostic.go").write_text("""package feed

import "time"

type diagnosticSelectionKey struct{}
type diagnosticSelection struct {
    include map[uint]bool
    observeKeys func(time.Duration)
}
""")
    start = '\tkeys := make([]string, 0, len(authors)+1)'
    replace(feed / "following.go", start, """\tmeasureStart := time.Now()
\tdiagnostic, _ := ctx.Value(diagnosticSelectionKey{}).(diagnosticSelection)
\tkeyCapacity := len(authors) + 1
\tif diagnostic.include != nil {
\t\tkeyCapacity = len(diagnostic.include) + 1
\t}
\tkeys := make([]string, 0, keyCapacity)""")
    replace(feed / "following.go", '\tfor _, authorID := range authors {\n',
            """\tfor _, authorID := range authors {
\t\tif diagnostic.include != nil && !diagnostic.include[authorID] {
\t\t\tcontinue
\t\t}
""")
    replace(feed / "following.go", '\n\tvar before string\n', """
\tif diagnostic.observeKeys != nil {
\t\tdiagnostic.observeKeys(time.Since(measureStart))
\t}

\tvar before string
""")
    redis = backend / "internal/middleware/redis"
    (redis / "push_diagnostic.go").write_text("""package redis

type DiagnosticBuildProbeKey struct{}
""")
    replace(redis / "zset.go", '\t\tfor _, key := range keys {\n\t\t\tranges = append(ranges, pipe.ZRevRangeByLex(ctx, key, rangeBy))\n\t\t}\n',
            """\t\tbuildStart := time.Now()
\t\tfor _, key := range keys {
\t\t\tranges = append(ranges, pipe.ZRevRangeByLex(ctx, key, rangeBy))
\t\t}
\t\tif observe, ok := ctx.Value(DiagnosticBuildProbeKey{}).(func(time.Duration)); ok {
\t\t\tobserve(time.Since(buildStart))
\t\t}
""")
    return backend


def write_result(out, name, rows):
    if not rows:
        return
    with (out / name).open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="new directory for this one matrix")
    parser.add_argument("--functional-only", action="store_true")
    args = parser.parse_args()
    out = args.output.resolve()
    # Existing evidence stays readable and a failed invocation cannot rerun a cell.
    out.mkdir(parents=True, exist_ok=False)
    for var in ("FEED_READ_TEST_DSN", "C_TEST_REDIS_ADDR"):
        if not os.environ.get(var):
            parser.error(f"{var} is required")
    revision = subprocess.check_output(["git", "rev-parse", "main"], cwd=ROOT, text=True).strip()
    if revision != BASE:
        parser.error(f"main is {revision}, expected {BASE}")
    both = []
    attempts = []
    with tempfile.TemporaryDirectory(prefix="gofeed-push-diagnostic-") as scratch:
        backend = prepare(Path(scratch))
        binary = Path(scratch) / "feed.test"
        build = subprocess.run(["go", "test", "-c", "-o", binary, "./internal/feed"],
                               cwd=backend, text=True, capture_output=True)
        (out / "build.log").write_text(build.stdout + build.stderr)
        if build.returncode:
            raise SystemExit(build.returncode)
        (out / "build.json").write_text(json.dumps({
            "base": BASE, "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
            "go": subprocess.check_output(["go", "version"], cwd=backend, text=True).strip(),
            "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "test_sha256": hashlib.sha256((HERE / "push_diagnostic_test.go").read_bytes()).hexdigest(),
        }, indent=2) + "\n")

        def invoke(test, name, env):
            with (out / (name + ".log")).open("w") as log:
                proc = subprocess.run([binary, "-test.v", "-test.timeout=60s", "-test.run", f"^{test}$"],
                                      cwd=backend, env={**os.environ, **env}, stdout=log, stderr=subprocess.STDOUT)
            return proc.returncode

        gate = invoke("TestPushDiagnosticHistory", "history", {})
        if gate:
            raise SystemExit(f"functional preflight failed; see {out / 'history.log'}. No performance cell attempted.")
        if args.functional_only:
            return
        for k, variant, repeat in CELLS:
            label = f"k{k}-{variant}-r{repeat}"
            attempts.append({"authors": k, "variant": variant, "repeat": repeat, "status": "started"})
            write_result(out, "attempts.csv", attempts)
            print(label, flush=True)
            status = invoke("TestPushDiagnosticRound", label, {
                "DIAGNOSTIC_K": str(k), "DIAGNOSTIC_VARIANT": variant, "DIAGNOSTIC_REPEAT": str(repeat),
                "DIAGNOSTIC_OUTPUT": str(out / label),
            })
            raw = (out / (label + ".log")).read_text()
            matches = re.findall(r"DIAGNOSTIC_RESULT (\{[^\n]+\})", raw)
            if len(matches) == 1:
                result = json.loads(matches[0])
                result["valid_round"] = status == 0
                both.append(result)
            attempts[-1]["status"] = "pass" if status == 0 and len(matches) == 1 else "failed"
            write_result(out, "attempts.csv", attempts)
            write_result(out, "rounds.csv", both)
            for suffix in ("cpu.pb.gz", "alloc-after.pb.gz"):
                profile = out / f"{label}.{suffix}"
                if profile.is_file():
                    cmd = ["go", "tool", "pprof", "-top", "-nodecount=25"]
                    if suffix.startswith("alloc"):
                        cmd += ["-sample_index=alloc_space", "-base", out / f"{label}.alloc-before.pb.gz"]
                    with (out / f"{label}.{suffix.split('.')[0]}.txt").open("w") as report:
                        subprocess.run(cmd + [binary, profile], cwd=backend, stdout=report,
                                       stderr=subprocess.STDOUT, check=False)
    if any(row["status"] != "pass" for row in attempts):
        raise SystemExit("at least one cell failed; attempts and completed measurements preserved")


if __name__ == "__main__":
    main()
