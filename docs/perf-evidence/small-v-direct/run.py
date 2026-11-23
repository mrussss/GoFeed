#!/usr/bin/env python3
"""Run the eight small-V comparisons once against dedicated loopback services."""

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

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
BASE = "71f9713b394c90ed1d0b0d2334566860cd6207bf"
CELLS = [(k, mode, repeat) for k in (2, 100)
         for repeat, order in ((1, ("push", "pull")), (2, ("pull", "push")))
         for mode in order]


def replace_once(file, before, after):
    text = file.read_text()
    if text.count(before) != 1:
        raise ValueError(f"unexpected source anchor: {file}")
    file.write_text(text.replace(before, after))


def prepare(work):
    archive = subprocess.check_output(["git", "archive", BASE, "backend"], cwd=ROOT)
    with tarfile.open(fileobj=io.BytesIO(archive)) as source:
        source.extractall(work, filter="data")
    backend = work / "backend"
    feed = backend / "internal/feed/following.go"
    replace_once(feed, '''	keys := make([]string, 0, len(authors)+1)
	keys = append(keys, f.rediscache.Key("feed:inbox:%d", viewerID))
	for _, authorID := range authors {
		// A push author may still have videos written while in pull mode.
		keys = append(keys, f.rediscache.Key("feed:author_outbox:%d", authorID))
	}''', '''	spec, _ := ctx.Value(smallVReadKey{}).(smallVReadSpec)
	capacity := len(authors)
	if spec.mode == "push" {
		capacity = 1
	}
	keys := make([]string, 0, capacity)
	if spec.mode != "pull" {
		keys = append(keys, f.rediscache.Key("feed:inbox:%d", viewerID))
	}
	if spec.mode != "push" {
		for _, authorID := range authors {
			keys = append(keys, f.rediscache.Key("feed:author_outbox:%d", authorID))
		}
	}''')
    start = "func (f *FeedService) followingFromMySQL(ctx context.Context, count int, viewerID uint, cursor *TimeCursor) ([]*video.Video, error) {"
    replace_once(feed, start, start + '''
	if spec, ok := ctx.Value(smallVReadKey{}).(smallVReadSpec); ok && spec.observeFallback != nil {
		spec.observeFallback()
	}''')
    (feed.parent / "smallv_diagnostic.go").write_text('''package feed

import "context"

type smallVReadKey struct{}

type smallVReadSpec struct {
    mode string
    observeFallback func()
}

func SmallVReadContext(ctx context.Context, mode string, observeFallback func()) context.Context {
    return context.WithValue(ctx, smallVReadKey{}, smallVReadSpec{mode, observeFallback})
}
''')
    video = backend / "internal/video/author_feed.go"
    mode = "	mode := distributionMode(state.DistributionMode, followers, vs.followingFeed)"
    replace_once(video, mode, mode + '''
	if forced, ok := ctx.Value(smallVPublishKey{}).(DistributionMode); ok {
		mode = forced
	}''')
    (video.parent / "smallv_diagnostic.go").write_text('''package video

import "context"

type smallVPublishKey struct{}

func SmallVPublishContext(ctx context.Context, mode DistributionMode) context.Context {
    return context.WithValue(ctx, smallVPublishKey{}, mode)
}
''')
    harness = backend / "internal/worker/smallv_direct_test.go"
    harness.write_bytes((HERE / "smallv_direct_test.go").read_bytes())
    return backend


def write_csv(path, rows):
    if not rows:
        return
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="new output directory")
    parser.add_argument("--preflight-only", action="store_true")
    args = parser.parse_args()
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    if not all(os.environ.get(name) for name in ("SMALLV_MYSQL_DSN", "SMALLV_REDIS_ADDR")):
        parser.error("set SMALLV_MYSQL_DSN and SMALLV_REDIS_ADDR")
    branch = subprocess.check_output(["git", "rev-parse", "main"], cwd=ROOT, text=True).strip()
    if branch != BASE:
        parser.error(f"main is {branch}, expected {BASE}")
    attempts, writes, reads = [], [], []
    with tempfile.TemporaryDirectory(prefix="gofeed-smallv-") as tmp:
        backend = prepare(Path(tmp))
        binary = Path(tmp) / "worker.test"
        result = subprocess.run(["go", "test", "-c", "-o", binary, "./internal/worker"],
                                cwd=backend, capture_output=True, text=True)
        (out / "build.log").write_text(result.stdout + result.stderr)
        if result.returncode:
            raise SystemExit(result.returncode)
        (out / "build.json").write_text(json.dumps({
            "base": BASE, "binary_sha256": digest(binary), "runner_sha256": digest(Path(__file__)),
            "test_sha256": digest(HERE / "smallv_direct_test.go"),
            "go": subprocess.check_output(["go", "version"], cwd=backend, text=True).strip(),
        }, indent=2) + "\n")

        def invoke(name, filename, overrides):
            with (out / filename).open("w") as log:
                return subprocess.run([binary, "-test.v", "-test.timeout=120s", "-test.run", f"^{name}$"],
                                      cwd=backend, env={**os.environ, **overrides}, stdout=log,
                                      stderr=subprocess.STDOUT).returncode

        if invoke("TestSmallVEnvironment", "environment.log", {}):
            raise SystemExit("environment gate failed before any configuration was attempted")
        if args.preflight_only:
            return
        for k, mode, repeat in CELLS:
            label = f"k{k}-{mode}-r{repeat}"
            attempts.append({"authors": k, "mode": mode, "repeat": repeat, "status": "started"})
            write_csv(out / "attempts.csv", attempts)
            print(label, flush=True)
            status = invoke("TestSmallVDirect", label + ".log", {
                "SMALLV_K": str(k), "SMALLV_MODE": mode, "SMALLV_REPEAT": str(repeat),
                "SMALLV_EVENTS": str(out / (label + "-events.csv")),
            })
            log = (out / (label + ".log")).read_text()
            attempts[-1]["status"] = "pass" if status == 0 else f"failed({status})"
            for marker, rows, file in (("SMALLV_WRITE", writes, "write-rounds.csv"), ("SMALLV_READ", reads, "read-rounds.csv")):
                emitted = re.findall(rf"{marker} (\{{[^\n]+\}})", log)
                if len(emitted) == 1:
                    rows.append({**json.loads(emitted[0]), "round_pass": status == 0})
                    write_csv(out / file, rows)
                elif status == 0:
                    attempts[-1]["status"] = "failed(missing measurement)"
            write_csv(out / "attempts.csv", attempts)
    if any(row["status"] != "pass" for row in attempts):
        raise SystemExit("failed attempts are recorded; no cell was retried")


if __name__ == "__main__":
    main()
