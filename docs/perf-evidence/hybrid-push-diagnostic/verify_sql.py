#!/usr/bin/env python3
"""Verify the SQL counter against MySQL without running a performance round."""

import os
from pathlib import Path
import subprocess
import tempfile

from run import prepare

BEFORE = '\tcheckAll("current", current)\n\tcheckAll("history-aware", selected)'
AFTER = BEFORE + '''
\tvar callbacks, dryRuns int
\tobserve := func(query *gorm.DB) {
\t\tcallbacks++
\t\tif query.DryRun {
\t\t\tdryRuns++
\t\t}
\t}
\tdiagMust(t, tx.Callback().Query().After("gorm:query").Register("diagnostic:audit-query", observe))
\tdiagMust(t, tx.Callback().Row().After("gorm:row").Register("diagnostic:audit-row", observe))
\tselectCount := func() int {
\t\trows, err := tx.Statement.ConnPool.QueryContext(ctx, "SHOW SESSION STATUS LIKE 'Com_select'")
\t\tdiagMust(t, err)
\t\tdefer rows.Close()
\t\tif !rows.Next() {
\t\t\tt.Fatal("MySQL Com_select missing")
\t\t}
\t\tvar label string
\t\tvar count int
\t\tdiagMust(t, rows.Scan(&label, &count))
\t\treturn count
\t}
\tfor _, pair := range []struct { label string; context context.Context }{
\t\t{"current", current}, {"history-aware", selected},
\t} {
\t\tcallbacks, dryRuns = 0, 0
\t\tstart := selectCount()
\t\tpage, err := service.ListByFollowing(pair.context, 3, nil, viewer)
\t\tdiagMust(t, err)
\t\tif !page.HasMore || len(page.VideoList) != 3 {
\t\t\tt.Fatal("unexpected warm page")
\t\t}
\t\tactual := selectCount() - start
\t\tt.Logf("%s SQL audit: callbacks=%d dry-run=%d actual MySQL SELECT=%d", pair.label, callbacks, dryRuns, actual)
\t\tif actual != 3 || callbacks != 4 || dryRuns != 1 {
\t\t\tt.Fatal("SQL counter mismatch")
\t\t}
\t}
'''


def main():
    for env_name in ("FEED_READ_TEST_DSN", "C_TEST_REDIS_ADDR"):
        if not os.environ.get(env_name):
            raise SystemExit(f"{env_name} is required")
    with tempfile.TemporaryDirectory(prefix="gofeed-push-sql-") as work:
        backend = prepare(Path(work))
        harness = backend / "internal/feed/push_diagnostic_test.go"
        contents = harness.read_text()
        if contents.count(BEFORE) != 1:
            raise SystemExit("SQL witness insertion anchor must occur once")
        harness.write_text(contents.replace(BEFORE, AFTER))
        subprocess.run(["go", "test", "-count=1", "-v", "./internal/feed",
                        "-run", "^TestPushDiagnosticHistory$"], cwd=backend, check=True)


if __name__ == "__main__":
    main()
