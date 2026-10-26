# SQL

SQL prepares the PostgreSQL source for Vector and Graph. It downloads a Stack
Overflow dump, extracts five XML tables, and imports their rows. The snapshot
becomes available to downstream builds after its database counts match the XML
parser counts and publication succeeds.

Run `download`, `extract`, `import`, and `validate` separately, or use `run` for
the complete procedure. Use `status` to read local progress. To resume an
interrupted import, repeat `import`; it continues after the last committed batch.

## Usage

Configure the application and PostgreSQL, then run these commands from the
repository root. Replace `SNAPSHOT_ID` with the dump identifier.

```bash
.venv/bin/python -B -m sql.start download --snapshot SNAPSHOT_ID --download-method http
.venv/bin/python -B -m sql.start extract --snapshot SNAPSHOT_ID
.venv/bin/python -B -m sql.start import --snapshot SNAPSHOT_ID
.venv/bin/python -B -m sql.start validate --snapshot SNAPSHOT_ID
.venv/bin/python -B -m sql.start status --json
```

Use `latest` to discover a release when downloading, then keep its resolved ID
for later builds. A full `run` rebuilds existing PostgreSQL output only when you
supply `--overwrite`.

Read the [full SQL procedure](../../doc/sql.md) for setup, source verification,
import rules, configuration and recovery. Server settings belong in the
[PostgreSQL deployment](../../deployment/postgres/README.md).
