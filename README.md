# Airflow RO-Crate

This project is a BSc thesis prototype for creating reproducibility packages from Apache Airflow DAG runs.

After a DAG has run, the tool reads Airflow's metadata database, collects the DAG source, task metadata, logs, optional data files, and Docker worker environment evidence, then writes everything into one `.tar.gz` package with RO-Crate JSON-LD metadata.

The current implementation is focused on a local Docker Compose Airflow setup using CeleryExecutor.

## What It Captures

For one DAG run, the package can include:

- the Airflow DAG source file
- DAG run metadata from the Airflow database
- task instances, task states, operators, start/end times, and hostnames
- XCom values
- machine readable parallel task overlap information
- local Airflow task logs, when available
- data files or folders passed with `--include-data`
- Docker Compose setup files
- worker container evidence:
  - `worker-pip-freeze.txt`
  - `worker-python-version.txt`
  - `worker-os-release.txt`

The package does not promise one click reproduction on every machine. The goal is to make the original run easier to inspect, understand, archive, and run again in a compatible Airflow environment.

## Quick Start

Start the local Airflow stack:

```bash
docker compose up -d
```

Open Airflow:

```text
http://localhost:8080
username: airflow
password: airflow
```

Trigger a DAG in the UI, wait until it finishes, then capture it:

```bash
airflow-rocrate capture --dag-id my_dag
```

To include data files:

```bash
airflow-rocrate capture \
  --dag-id my_dag \
  --include-data data/sample_measurements.csv
```

To capture a specific run:

```bash
airflow-rocrate capture \
  --dag-id my_dag \
  --run-id "manual__2026-05-04T13:05:26.792569+00:00"
```

## Database URL

The CLI needs access to the Airflow metadata database.

In the included Docker Compose setup, the database URL from the host machine is:

```text
postgresql+psycopg2://airflow:airflow@localhost:5432/airflow
```

The CLI resolves the database URL in this order:

1. `--db-url`
2. `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`
3. local Docker Compose default when `docker-compose.yaml` is present
4. `airflow config get-value database sql_alchemy_conn`
5. final local default

If automatic detection does not match your setup, pass it directly:

```bash
airflow-rocrate capture \
  --dag-id my_dag \
  --db-url postgresql+psycopg2://user:pass@localhost:5432/airflow
```

## CLI Commands

List recent runs for a DAG:

```bash
airflow-rocrate list-runs --dag-id my_dag
```

Capture the latest successful run:

```bash
airflow-rocrate capture --dag-id my_dag
```

Capture a specific run:

```bash
airflow-rocrate capture --dag-id my_dag --run-id <run_id>
```

Inspect a package archive:

```bash
tar -xzf reproducibility-package_my_dag_<run_id>.tar.gz
cd reproducibility-package_my_dag_<run_id>
```

Show CLI information:

```bash
airflow-rocrate info
```

## Package Layout

A generated package looks like this:

```text
reproducibility-package_<dag_id>_<run_id>/
├── ro-crate-metadata.json
├── workflow/
│   └── dags/
│       └── my_dag.py
├── data/
├── logs/
│   └── airflow-task-logs/
├── environment/
│   ├── Dockerfile
│   ├── docker-compose.yaml
│   ├── requirements.txt
│   ├── system.json
│   ├── worker-pip-freeze.txt
│   ├── worker-python-version.txt
│   └── worker-os-release.txt
├── experiment_setup/
│   └── airflow_setup.md
├── MANIFEST.json
└── README.md
```

The most important files are:

- `ro-crate-metadata.json`: machine readable RO-Crate JSON-LD metadata
- `MANIFEST.json`: simpler summary of the captured run
- `workflow/dags/`: the DAG source code
- `logs/airflow-task-logs/`: copied Airflow task logs
- `environment/`: Docker, Python, and worker runtime evidence
- `data/`: user-provided data files

## Docker Scope

This prototype is intentionally focused on Docker.

The Airflow database metadata capture is mostly independent of the executor, because Airflow stores DAG runs and task instances in the metadata database. Runtime environment capture is different: it currently assumes a Docker Compose worker service such as `airflow-worker`.

That is why the tool can run:

```bash
docker compose exec -T airflow-worker python --version
docker compose exec -T airflow-worker python -m pip freeze
docker compose exec -T airflow-worker cat /etc/os-release
```

Other Airflow deployments would need different environment capture logic:

- `LocalExecutor`: inspect the local Airflow Python environment
- `SequentialExecutor`: inspect the scheduler/local process environment
- Celery workers on VMs: inspect the worker host or VM
- `KubernetesExecutor`: inspect the task pod image/spec

So the honest scope is:

> Validated for Docker Compose Airflow with CeleryExecutor. Other setups may work for database metadata capture, but runtime environment capture depends on the deployment.

## Development

Install dependencies:

```bash
uv pip install -e ".[dev]"
```

Run linting:

```bash
uv run ruff check src dags
```

## Main Source Files

```text
src/airflow_rocrate/
├── cli.py
├── metadata.py
├── rocrate_generator.py
├── packager.py
└── __init__.py
```

- `cli.py`: command line interface
- `metadata.py`: reads the Airflow metadata database
- `rocrate_generator.py`: maps Airflow metadata to RO-Crate JSON-LD
- `packager.py`: builds the final `.tar.gz` package
- `__init__.py`: package exports

## RO-Crate

RO-Crate is a lightweight format for describing research objects using JSON-LD metadata.

This project uses RO-Crate to describe an Airflow DAG run as a research object: workflow code, execution metadata, logs, data references, and environment evidence.

Learn more: https://w3id.org/ro/crate/

## Status

The prototype is working for the local Docker Compose Airflow setup used in this project.

Verified behavior includes:

- package creation as `.tar.gz`
- data inclusion with `--include-data`
- worker environment capture from Docker Compose
- task log inclusion
- parallel task overlap metadata
- XCom provenance parsing from JSON strings
