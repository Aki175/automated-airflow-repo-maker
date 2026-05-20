"""
Command line entry points for building Airflow reproducibility packages.
"""

import click
from pathlib import Path
from typing import Any, Optional
import os
import subprocess
import sys
import tempfile

from airflow_rocrate import __version__


DEFAULT_LOCAL_DB_URL = "postgresql+psycopg2://airflow:airflow@localhost:5432/airflow"


@click.group()
@click.version_option(version=__version__)
def cli():
    """Build reproducibility packages from Airflow DAG runs."""
    pass


@cli.command()
@click.option(
    "--db-url",
    required=False,
    help="Airflow metadata database URL (postgresql+psycopg2://user:pass@host/db)",
)
@click.option(
    "--dag-id",
    required=True,
    help="Airflow DAG ID to capture.",
)
@click.option(
    "--run-id",
    required=False,
    help="Airflow DAG run ID. If omitted, the latest successful run is captured.",
)
@click.option(
    "--output-dir",
    type=click.Path(),
    default=".",
    help="Output directory for the package (default: current directory)",
)
@click.option(
    "--include-data",
    type=click.Path(exists=True),
    multiple=True,
    help="Data files or directories to include in the package",
)
def capture(
    db_url: Optional[str],
    dag_id: str,
    run_id: Optional[str],
    output_dir: str,
    include_data: tuple,
):
    """
    Capture one DAG run and write a package for it.
    """
    click.echo(" Capturing Airflow DAG execution...")
    click.echo(f"   DAG ID: {dag_id}")

    db_url = _resolve_db_url(db_url)

    click.echo(f"   Database: {db_url.split('@')[1] if '@' in db_url else 'local'}")
    click.echo(f"   Output folder: {Path(output_dir).resolve()}")

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    try:
        from airflow_rocrate import (
            AirflowMetadataCapture,
            RocrateCrateGenerator,
            ReproducibilityPackager,
        )

        with tempfile.TemporaryDirectory(prefix="airflow-rocrate-capture-") as captured_tmp:
            captured_data_dir = Path(captured_tmp)

            click.echo("\nCapturing metadata from Airflow database...")
            with AirflowMetadataCapture(db_url) as capture:
                if not run_id:
                    run_id = _latest_run_id(capture, dag_id)
                    click.echo(f"   Latest successful run: {run_id}")
                else:
                    click.echo(f"   Run ID: {run_id}")

                metadata = capture.capture_full_execution(
                    dag_id=dag_id,
                    run_id=run_id,
                    output_dir=captured_data_dir,
                )

            click.echo(" Metadata captured")

            data_files = [Path(f) for f in include_data] if include_data else None
            dag_source_file = _find_dag_source_file(dag_id, captured_data_dir)
            dag_files = [dag_source_file] if dag_source_file else None
            logs_dir = _find_logs_dir(dag_id, run_id)

            if dag_source_file:
                click.echo(f"   DAG source: {dag_source_file}")
            else:
                click.echo("   DAG source: not found locally; continuing with database metadata only")
            if logs_dir:
                click.echo(f"   Airflow logs: {logs_dir}")

            with tempfile.TemporaryDirectory(prefix="airflow-rocrate-") as rocrate_tmp:
                click.echo("\n  Generating RO-Crate metadata...")
                rocrate_gen = RocrateCrateGenerator(Path(rocrate_tmp))

                rocrate_dir = rocrate_gen.create_rocrate_from_metadata(
                    dag_id=dag_id,
                    run_id=run_id,
                    metadata=metadata,
                    dag_files=dag_files,
                    data_files=data_files,
                )
                click.echo(" RO-Crate metadata created")

                click.echo("\n Creating reproducibility package...")
                packager = ReproducibilityPackager(output_path)

                package_path = packager.create_package(
                    dag_id=dag_id,
                    run_id=run_id,
                    metadata=metadata,
                    rocrate_dir=rocrate_dir,
                    dag_source_file=dag_source_file,
                    data_files=data_files,
                    logs_dir=logs_dir,
                )

        click.echo(f"✓ Package created: {package_path}")
        click.echo("\n Success! Reproducibility package ready:")
        click.echo(f"    {package_path}")
        click.echo("\n   To inspect it:")
        click.echo(f"   $ tar -xzf {package_path.name}")

    except Exception as e:
        click.echo(f" Error: {e}", err=True)
        click.echo("\nTroubleshooting:")
        click.echo("- Ensure Airflow database is accessible")
        click.echo("- Check DAG ID and Run ID are correct")
        click.echo("- Verify database URL format: postgresql+psycopg2://user:pass@host/db")
        sys.exit(1)


@cli.command()
@click.option(
    "--dag-id",
    required=True,
    help="Airflow DAG ID.",
)
@click.option(
    "--db-url",
    required=False,
    help="Airflow metadata database URL",
)
def list_runs(dag_id: str, db_url: Optional[str]):
    """List recent runs for one DAG."""
    click.echo(f" Listing runs for DAG: {dag_id}")

    db_url = _resolve_db_url(db_url)

    try:
        from airflow_rocrate import AirflowMetadataCapture

        with AirflowMetadataCapture(db_url) as capture:
            runs = capture.get_dag_runs(dag_id, limit=20)

        if not runs:
            click.echo(f" No runs found for DAG: {dag_id}")
            return

        click.echo(f"\n✓ Found {len(runs)} runs:")
        click.echo()

        for run in runs:
            status_emoji = (
                "✅" if run["state"] == "success"
                else "❌" if run["state"] == "failed"
                else "⌛"
            )
            duration = f" ({run['duration_seconds']:.1f}s)" if run['duration_seconds'] else ""
            click.echo(f"{status_emoji} {run['run_id']}{duration}")
            click.echo(f"   State: {run['state']}")
            click.echo(f"   Started: {run['start_date']}")

    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option(
    "--package",
    type=click.Path(exists=True),
    required=True,
    help="Path to reproducibility package (.tar.gz)",
)
@click.option(
    "--extract-to",
    type=click.Path(),
    default="./",
    help="Directory to extract package to (default: current directory)",
)
def extract(package: str, extract_to: str):
    """Extract a package created by this tool."""
    click.echo("📦 Extracting reproducibility package...")
    click.echo(f"   Package: {package}")
    click.echo(f"   Extract to: {extract_to}")

    try:
        from airflow_rocrate import ReproducibilityPackager

        packager = ReproducibilityPackager(extract_to)
        extract_path = packager.extract_package(Path(package), Path(extract_to))

        click.echo("\n Package extracted successfully!")
        click.echo(f"    Location: {extract_path}")
        click.echo("\n   Next steps:")
        click.echo(f"   $ cd {extract_path}")
        click.echo("   $ cat README.md")
        click.echo("   $ airflow dags trigger <dag_id>")

    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        sys.exit(1)


@cli.command()
def info():
    """Show a short summary of the tool."""
    click.echo(f"""
╔═══════════════════════════════════════════════════════════╗
║      Airflow RO-Crate Reproducibility Package             ║
║           Automated Generator v{__version__}                      ║
╚═══════════════════════════════════════════════════════════╝

Overview
  Capture Apache Airflow DAG executions and generate RO-Crate
  metadata inside reproducibility packages.

Key Features
  • Automatic metadata capture from Airflow database
  • RO-Crate JSON-LD metadata
  • Include DAG source code, logs, and data
  • Portable package structure focused on Docker
  • Environment, logs, workflow source, and setup notes

Commands
  capture       Create reproducibility package from DAG run
  list-runs     List available DAG executions
  extract       Extract and use reproducibility package
  info          Show this message

Documentation
  RO-Crate:     https://w3id.org/ro/crate/
  Airflow:      https://airflow.apache.org/docs/

Quick Start
  1. airflow-rocrate list-runs --dag-id my_dag
  2. airflow-rocrate capture --dag-id my_dag
  3. tar -xzf <package.tar.gz>

Environment Variables
  AIRFLOW__DATABASE__SQL_ALCHEMY_CONN  Airflow database URL

Database URL Resolution
  The capture and list-runs commands need access to the Airflow metadata DB.
  Resolution order:
    1. --db-url
    2. AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
    3. local Docker Compose fallback when docker-compose.yaml is present:
       postgresql+psycopg2://airflow:airflow@localhost:5432/airflow
    4. airflow config get-value database sql_alchemy_conn
    5. local default:
       postgresql+psycopg2://airflow:airflow@localhost:5432/airflow

""")


def _find_dag_source_file(dag_id: str, captured_data_dir: Path) -> Optional[Path]:
    """Find the DAG file that should go into the package."""
    candidates = [
        captured_data_dir / f"{dag_id}_source.py",
        Path.cwd() / "dags" / f"{dag_id}.py",
    ]

    airflow_home = os.getenv("AIRFLOW_HOME")
    if airflow_home:
        candidates.append(Path(airflow_home) / "dags" / f"{dag_id}.py")

    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate

    return None


def _find_logs_dir(dag_id: str, run_id: str) -> Optional[Path]:
    """Find task logs when Airflow logs are available on disk."""
    candidates = [
        Path.cwd() / "logs" / f"dag_id={dag_id}" / f"run_id={run_id}",
        Path.cwd() / "logs" / dag_id / run_id,
    ]

    airflow_home = os.getenv("AIRFLOW_HOME")
    if airflow_home:
        candidates.append(Path(airflow_home) / "logs" / f"dag_id={dag_id}" / f"run_id={run_id}")

    for candidate in candidates:
        if candidate.exists() and candidate.is_dir():
            return candidate

    return None


def _resolve_db_url(db_url: Optional[str]) -> str:
    """Choose the Airflow database URL."""
    if db_url:
        return db_url

    env_db_url = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
    if env_db_url:
        return env_db_url

    if (Path.cwd() / "docker-compose.yaml").exists():
        return DEFAULT_LOCAL_DB_URL

    return (
        _airflow_config_db_url()
        or DEFAULT_LOCAL_DB_URL
    )


def _airflow_config_db_url() -> Optional[str]:
    """Read the database URL from the local Airflow config if possible."""
    try:
        completed = subprocess.run(
            ["airflow", "config", "get-value", "database", "sql_alchemy_conn"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except Exception:
        return None

    if completed.returncode != 0:
        return None

    candidates = [
        line.strip()
        for line in completed.stdout.splitlines()
        if line.strip() and "://" in line
    ]
    return candidates[-1] if candidates else None


def _latest_run_id(capture: Any, dag_id: str) -> str:
    """Pick the latest successful run, or the newest run when none succeeded."""
    runs = capture.get_dag_runs(dag_id, limit=20)
    if not runs:
        raise click.ClickException(
            f"No DAG runs found for {dag_id}. Trigger the DAG in the Airflow UI first."
        )

    successful_runs = [run for run in runs if run.get("state") == "success"]
    selected = successful_runs[0] if successful_runs else runs[0]

    if selected.get("state") != "success":
        click.echo(
            f"   Warning: latest run is {selected.get('state')}; package may describe an incomplete run.",
            err=True,
        )

    return selected["run_id"]


def main():
    """Run the command line interface."""
    cli()


if __name__ == "__main__":
    main()
