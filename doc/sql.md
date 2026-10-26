# SQL procedure

SQL publishes the PostgreSQL dataset used by Vector and Graph. It downloads a
Stack Overflow release, verifies the archive, extracts the supported XML tables,
and imports their rows. Downstream builds can open the snapshot after validation
and publication succeed.

## Setup

Use Linux, Python 3.14, and UV. Install native 7-Zip 26.04 or newer, preferably
`7zz`. Install `aria2c` if you choose torrent downloads; HTTP works without it.
Import and validation require a reachable PostgreSQL 18 server.

Run the examples from the repository root. Install the project packages with:

```bash
uv sync --python python3.14 --no-python-downloads
```

For a new setup, copy the configuration files:

```bash
cp .env.example .env
cp deployment/postgres/.env.example deployment/postgres/.env
```

Preserve existing configuration files. Set application paths and the PostgreSQL
connection in the root `.env`. For the bundled server, set storage, listener, and
credentials in `deployment/postgres/.env`. Application credentials must match
the server. You can also use an existing PostgreSQL 18 server.

Follow the [PostgreSQL setup](../deployment/postgres/README.md) to create the
external storage directories and password file, then start the bundled server:

```bash
./deployment/postgres/run.sh up -d --wait
```

Download and extraction use CPU, RAM, and disk, with no GPU requirement. Allow
space for the archive, extracted XML, database, indexes, and temporary files.
The pipeline checks available disk space during processing.

## Snapshot selection and verification

A snapshot uses its Internet Archive item ID. Selecting `latest` discovers a
release and records its identity. Archive size, checksums, and publisher evidence
tie later stages to that release. Extraction and import use the recorded local
identity.

Choose HTTP or torrent explicitly for `download` and `run`:

```bash
.venv/bin/python -B -m sql.start download --snapshot SNAPSHOT_ID --download-method http
```

Replace `SNAPSHOT_ID` with an item ID, or use `latest` to discover one. If you omit
`--download-method` in a terminal, the command prompts for a choice. Supply it
explicitly in unattended jobs. A failed transfer does not switch methods.

The pipeline verifies a completed archive before reusing it. A new download
invocation discards stale partial-transfer state. Extraction records the selected
XML files and the verified archive they came from; it rejects an inventory from
a different release.

## Stages

| Command | Result |
| --- | --- |
| `download` | A verified archive and recorded source identity. |
| `extract` | The supported XML inputs and their inventory. |
| `import` | PostgreSQL rows with a committed progress ledger. |
| `validate` | Validation evidence and a published snapshot when the checks pass. |
| `run` | Download, extraction, import and validation in order. |
| `status` | Local snapshot state and writer-lock status. |

To run stages separately:

```bash
.venv/bin/python -B -m sql.start extract --snapshot SNAPSHOT_ID
.venv/bin/python -B -m sql.start import --snapshot SNAPSHOT_ID
.venv/bin/python -B -m sql.start validate --snapshot SNAPSHOT_ID
.venv/bin/python -B -m sql.start status --json
```

For a complete run:

```bash
.venv/bin/python -B -m sql.start run --snapshot SNAPSHOT_ID --download-method http
```

`--stop-after download`, `extract`, `import` or `validate` limits a full `run`.
Use a standalone stage command to continue afterward. Status reads local metadata
and lock state without connecting to PostgreSQL.

## Imported data

The importer handles these five tables:

| Table | Content |
| --- | --- |
| `Posts` | Questions, answers and other source post types, including tag documentation. |
| `Comments` | Comments attached to source posts. |
| `Tags` | Tag names, usage counts and documentation references. |
| `Votes` | Source vote records and their type codes. |
| `PostLinks` | Links and duplicate relationships between posts. |

The importer streams XML rows into PostgreSQL COPY in batches limited by row
count and bytes. Parsing can use several worker processes. A committed batch
records its database count and source position, so an interrupted standalone
import can resume after that batch.

Downstream workflows can read the original body HTML.

### PostgreSQL schema

Each snapshot has a separate PostgreSQL schema. The importer creates its tables
automatically. Publication adds indexes, vacuums the tables, and updates query
planner statistics.

- Table and column names are double-quoted to preserve mixed case.
- All integer columns use `BIGINT`, including IDs and type codes.
- String columns use unrestricted `TEXT`.
- Dates use validated `TEXT` to preserve their original spelling and up to nine
  fractional digits. PostgreSQL timestamps would round that precision to
  microseconds.
- Only `Id` is required. Missing XML attributes remain `NULL`, including scores
  and counts; columns have no zero defaults.
- Every table has nullable `ExtraAttributes JSONB` for unknown XML fields.
  `Comments` includes `ContentLicense`; `Tags` includes `IsModeratorOnly` and
  `IsRequired`.
- After import, each table receives a unique index on its non-null `Id` values.
  The tables do not declare primary keys.

The [table and index definitions](../src/sql/adapter/postgres/schema/) are SQL
reference files. Their column order, types, nullability, and defaults match
`TABLES` in `sql.domain.schema` and `COLUMN_TYPES` in
`sql.adapter.postgres.postgres_store`. The index file matches the indexes created
during publication.

### Manual schema setup

For a separate schema created by hand, open a PostgreSQL client and run the
following in the same session as the reference SQL files:

```sql
CREATE SCHEMA "YOUR_SCHEMA";
SET search_path TO "YOUR_SCHEMA";
```

Replace `YOUR_SCHEMA` with your schema name. Run each of the five table files
once, load the rows, then run `knowledge_graph_indexes.sql`. Table files fail if
their tables already exist. The index file can be rerun: its index names match
those used by the importer, so it does not add duplicate indexes to a published
snapshot. Run `ANALYZE "Posts"` to update statistics for that table.

For an existing imported snapshot, select its actual `snapshot_...` schema and
run only the index file. The reference files omit the catalogue, metadata,
progress, and publication state required by the application. Use the import and
validation commands above to create a snapshot that Vector and Graph can read.

## Validation and publication

When the parser reaches the end of an XML table, it records the total row count.
Validation compares it with the exact PostgreSQL count. Publication requires the
public `xml-postgres` check to pass.

Publication records the source identity and validation results. Vector and Graph
require those records when opening a snapshot, so imported rows alone are not a
completed build input.

Repeat `validate` to check a completed snapshot. Running standalone `import` on a
published snapshot checks its publication rather than appending duplicate rows.

## Configuration

Application commands find the root `.env` from the repository location. Exported
environment variables override file values. Select another application file with
`--env-file /absolute/path/to/alternative.env`.

| Setting | Purpose |
| --- | --- |
| `STACK2GRAPH_DATA_ROOT` | External root for application data and derived defaults. |
| `STACK2GRAPH_DATA_DIR` | Dump archives, source metadata and snapshot state. |
| `STACK2GRAPH_DATA_XML_PATH` | Extracted XML inputs. |
| `STACK2GRAPH_DATA_POSTGRES_PATH` | PostgreSQL storage root and local snapshot publication records. |
| `STACK2GRAPH_POSTGRES_HOST`, `PORT`, `USER`, `DATABASE`, `PASSWORD_FILE` | Application connection settings, each with the `STACK2GRAPH_POSTGRES_` prefix. |
| `STACK2GRAPH_POSTGRES_POOL_SIZE` | Application connection-pool size. |
| `STACK2GRAPH_XML_WORKERS` | XML workers: `auto` or a positive integer. |

Automatic XML worker selection calibrates per machine and table and saves the
result. CPU and memory limits still cap a fixed worker count. Command-line
resource options include `--threads`, `--memory-bytes`, `--batch-rows`,
`--batch-bytes`, `--record-bytes`, `--spill-bytes` and `--disk-reserve`.

The [root example](../.env.example) documents application values. Server resource
limits and PostgreSQL tuning belong in the
[deployment configuration](../deployment/postgres/.env.example).

## Files and recovery

With the default paths, archives live under `<data_dir>/archive/<snapshot-id>`, XML
under `<xml_root>/<snapshot-id>`, and local state under
`<data_dir>/snapshots/<snapshot-id>`. The snapshot state includes `manifest.json`,
source evidence and `pipeline.log`. Publication records live under
`<postgres_root>/snapshots/<snapshot-id>`; the bundled server stores its physical
cluster under `<postgres_root>/cluster`.

A writer lock permits one process to modify a local snapshot at a time. Resume
an interrupted import by rerunning `import` with the same snapshot ID and inputs.
After correcting a validation failure, rerun `validate`.

A full `run` starts database import from zero. If database output already exists,
it requires explicit replacement:

```bash
.venv/bin/python -B -m sql.start run --snapshot SNAPSHOT_ID --download-method http --overwrite
```

Overwrite rebuilds the snapshot database from XML and invalidates its previous
publication records. It can reuse the verified archive and completed XML inputs.

For a separate read-only count report on a completed snapshot:

```bash
uv run python -m validation.start postgres --snapshot SNAPSHOT_ID
```

Continue with [Vector](vector.md) or [Graph](graph.md) after publication. Both can
use the same source snapshot without requiring the other workflow to run.
