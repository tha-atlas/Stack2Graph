# PostgreSQL

PostgreSQL stores the imported Stack Overflow tables. Run these commands from
the repository root.

## Setup

Copy [`.env.example`](.env.example) to this directory's `.env` for a new setup.
Use that file for server storage, listener port, user, database, password file,
and Docker settings. Configure the application separately in the
[main `.env`](../../.env.example): credentials must match the server, and the host
and port must be reachable from the application. Run the service as your normal
Linux user with Docker access, without `sudo`.

Create `cluster` under `STACK2GRAPH_DATA_POSTGRES_PATH`, outside the repository.
Write only the database password to `STACK2GRAPH_POSTGRES_PASSWORD_FILE`. Restrict
access to the password file, using its actual path in this command:

```bash
chmod 600 /absolute/path/to/local-password-file
```

The service keeps its database in the `cluster` folder when restarted.

## Usage

```bash
./deployment/postgres/run.sh config --quiet
./deployment/postgres/run.sh up -d --wait
./deployment/postgres/run.sh ps
./deployment/postgres/run.sh --help
```

The commands validate configuration, start PostgreSQL, show service status, and
print help. By default, PostgreSQL accepts connections from this machine.

The launcher reads this directory's `.env` independently of application
configuration. To use another complete configuration, place `--env-file FILE`
before the command. Relative paths resolve from the current working directory.
Later files override earlier files; shell variables override file values.

The service `.env` sets `STACK2GRAPH_POSTGRES_CONTAINER_NAME=stack2graph-postgres`.
Use a different name for an isolated deployment, or omit the override to let
Compose derive the name from its project.
