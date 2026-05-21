"""
Build the final archive for an Airflow reproducibility package.
"""

import shutil
from importlib import metadata as importlib_metadata
import json
import platform
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
import tarfile
import tempfile


class ReproducibilityPackager:
    """Collect package files and write the final archive."""

    def __init__(self, base_output_dir: Optional[Path] = None):
        """Choose where finished packages will be written."""
        self.base_output_dir = Path(base_output_dir) if base_output_dir else Path.cwd()
        self.base_output_dir.mkdir(parents=True, exist_ok=True)

    def create_package(
        self,
        dag_id: str,
        run_id: str,
        metadata: Dict[str, Any],
        rocrate_dir: Path,
        dag_source_file: Optional[Path] = None,
        data_files: Optional[List[Path]] = None,
        logs_dir: Optional[Path] = None,
    ) -> Path:
        """Create a full package for one DAG run."""
        package_name = f"reproducibility-package_{self._safe_name(dag_id)}_{self._safe_name(run_id)}"

        with tempfile.TemporaryDirectory() as tmpdir:
            package_dir = Path(tmpdir) / package_name
            package_dir.mkdir(parents=True)

            self._create_package_structure(package_dir, dag_id, run_id, metadata)

            if rocrate_dir.exists():
                metadata_file = rocrate_dir / "ro-crate-metadata.json"
                if metadata_file.exists():
                    shutil.copy(str(metadata_file), str(package_dir / "ro-crate-metadata.json"))

            if dag_source_file and dag_source_file.exists():
                workflow_dags_dir = package_dir / "workflow" / "dags"
                workflow_dags_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy(str(dag_source_file), str(workflow_dags_dir / dag_source_file.name))

            if data_files:
                data_dir = package_dir / "data"
                data_dir.mkdir(exist_ok=True)
                for data_file in data_files:
                    if data_file.exists():
                        if data_file.is_file():
                            shutil.copy(str(data_file), str(data_dir / data_file.name))
                        elif data_file.is_dir():
                            shutil.copytree(str(data_file), str(data_dir / data_file.name))

            if logs_dir and Path(logs_dir).exists():
                shutil.copytree(str(logs_dir), str(package_dir / "logs" / "airflow-task-logs"), dirs_exist_ok=True)

            self._create_environment_files(package_dir, metadata)
            self._create_experiment_setup_files(package_dir, dag_id, run_id, metadata)

            self._create_readme(package_dir, dag_id, run_id, metadata, package_name)

            output_package = self.base_output_dir / package_dir.name
            return self._create_tar_gz(package_dir, output_package)

    def _create_package_structure(
        self,
        package_dir: Path,
        dag_id: str,
        run_id: str,
        metadata: Dict[str, Any]
    ):
        """Create the folders and manifest used by the package."""
        (package_dir / "data").mkdir(parents=True)
        (package_dir / "logs").mkdir(parents=True)
        (package_dir / "workflow").mkdir(parents=True)
        (package_dir / "environment").mkdir(parents=True)
        (package_dir / "experiment_setup").mkdir(parents=True)

        manifest = {
            "package_type": "airflow-rocrate",
            "version": "1.0",
            "created": datetime.now().isoformat(),
            "dag_id": dag_id,
            "run_id": run_id,
            "metadata": metadata,
        }

        manifest_file = package_dir / "MANIFEST.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)

    def _create_readme(
        self,
        package_dir: Path,
        dag_id: str,
        run_id: str,
        metadata: Dict[str, Any],
        package_name: str,
    ):
        """Write the README that travels with the package."""
        dag_info = metadata.get("dag_info", {})
        capture_time = metadata.get("capture_timestamp", "Unknown")

        readme_content = f"""
# Airflow DAG Reproducibility Package

This package documents one Apache Airflow DAG execution and bundles the files needed
to understand and run it again in a compatible Airflow setup.

It is inspired by RO-Crate reproducibility packages: the package does not magically
install a full Airflow platform, but it records the workflow, logs, software
requirements, setup notes, and machine readable RO-Crate metadata so another user can
prepare the correct environment and trigger the DAG again.

## DAG Information

- **DAG ID**: {dag_id}
- **Run ID**: {run_id}
- **Captured**: {capture_time}
- **Owner**: {dag_info.get('owner', 'Unknown')}
- **Description**: {dag_info.get('description', 'N/A')}

## Package Contents

```
.
├── ro-crate-metadata.json
├── workflow/
├── data/
├── logs/
├── environment/
├── experiment_setup/
├── MANIFEST.json
└── README.md
```

## Reproducing the Execution

### Prerequisites captured in this package

See:

- `environment/requirements.txt`
- `environment/system.json`
- `environment/worker-pip-freeze.txt` if a Docker Compose worker was available
- `environment/worker-python-version.txt` if a Docker Compose worker was available
- `environment/worker-os-release.txt` if a Docker Compose worker was available
- `environment/Dockerfile`
- `environment/docker-compose.yaml` if it was available in the source project
- `experiment_setup/airflow_setup.md`

```bash
pip install -r environment/requirements.txt
```

### Steps

1. Extract this package:
```bash
tar -xzf {package_name}.tar.gz
cd {package_name}
```

2. Review the RO-Crate metadata:
```bash
cat ro-crate-metadata.json
```

3. Copy or mount the DAG from `workflow/dags/` into an Airflow DAG folder.

4. Trigger the DAG:
```bash
airflow dags trigger {dag_id}
```

## RO-Crate

This package includes RO-Crate metadata in:

- `ro-crate-metadata.json`

RO-Crate is a lightweight, standard format for research object packaging and distribution.

See: https://w3id.org/ro/crate/

## Task Execution Summary

"""

        for task in metadata.get("task_instances", []):
            readme_content += f"\n- **{task['task_id']}**: {task['state']}"
            if task.get('duration_seconds'):
                readme_content += f" ({task['duration_seconds']:.2f}s)"

        readme_content += """

## Support

For issues or questions about reproducing this package, refer to:
- Apache Airflow Documentation: https://airflow.apache.org/docs/
- RO-Crate Specification: https://w3id.org/ro/crate/

## License

This reproducibility package is provided as is for scientific purposes.
"""

        readme_file = package_dir / "README.md"
        with open(readme_file, "w") as f:
            f.write(readme_content)

    def _create_environment_files(
        self,
        package_dir: Path,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """Write files that describe the software environment."""
        environment_dir = package_dir / "environment"
        requirements = self._project_requirements()
        (environment_dir / "requirements.txt").write_text("\n".join(requirements) + "\n")

        system_info = {
            "scope": "package_creation_host",
            "captured_python_version": sys.version,
            "captured_platform": platform.platform(),
            "captured_system": platform.system(),
            "captured_machine": platform.machine(),
            "note": (
                "This describes the environment where airflow-rocrate created the package. "
                "The Airflow worker runtime may be different; see worker-* files when available."
            ),
        }
        with open(environment_dir / "system.json", "w") as f:
            json.dump(system_info, f, indent=2)

        airflow_version = self._airflow_version(metadata)
        default_airflow_image = (
            f"apache/airflow:{airflow_version}"
            if airflow_version != "unknown"
            else "apache/airflow:latest"
        )
        dockerfile = f"""ARG AIRFLOW_IMAGE={default_airflow_image}
FROM ${{AIRFLOW_IMAGE}}

COPY environment/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY workflow/dags/ /opt/airflow/dags/
"""
        (environment_dir / "Dockerfile").write_text(dockerfile)

        docker_compose = Path.cwd() / "docker-compose.yaml"
        if docker_compose.exists():
            shutil.copy(str(docker_compose), str(environment_dir / "docker-compose.yaml"))

        self._capture_worker_environment(environment_dir)

    def _create_experiment_setup_files(
        self,
        package_dir: Path,
        dag_id: str,
        run_id: str,
        metadata: Dict[str, Any],
    ):
        """Write Airflow setup notes for the package."""
        setup_dir = package_dir / "experiment_setup"
        airflow_version = self._airflow_version(metadata)
        setup_notes = f"""# Airflow Experiment Setup

## Workflow System

- System: Apache Airflow
- Captured Airflow version: {airflow_version}
- DAG ID: {dag_id}
- Original run ID: {run_id}

## How To Prepare A Compatible Environment

1. Install or start Apache Airflow.
2. Install the Python packages listed in `../environment/requirements.txt`.
3. Copy `../workflow/dags/` into your Airflow DAGs folder.
4. Wait until Airflow parses the DAG.
5. Trigger the DAG with:

```bash
airflow dags trigger {dag_id}
```

## Captured Run Information

The original run metadata is stored in:

- `../MANIFEST.json`
- `../ro-crate-metadata.json`

## Dependency And Runtime Evidence

- Declared dependencies: `../environment/requirements.txt`
- Package creation host details: `../environment/system.json`
- Airflow worker Python packages, when available: `../environment/worker-pip-freeze.txt`
- Airflow worker Python version, when available: `../environment/worker-python-version.txt`
- Airflow worker OS details, when available: `../environment/worker-os-release.txt`
"""
        (setup_dir / "airflow_setup.md").write_text(setup_notes)

    def _create_tar_gz(self, source_dir: Path, output_path: Path) -> Path:
        """Create the tar.gz archive."""
        output_file = Path(str(output_path) + ".tar.gz")

        with tarfile.open(str(output_file), "w:gz") as tar:
            tar.add(str(source_dir), arcname=source_dir.name)

        return output_file

    def _safe_name(self, value: str) -> str:
        """Create a safe piece of a file name."""
        return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in str(value)).strip("_") or "unknown"

    def _project_requirements(self) -> List[str]:
        """Read the project dependencies to include in the package."""
        pyproject = Path.cwd() / "pyproject.toml"
        if pyproject.exists():
            with open(pyproject, "rb") as f:
                data = tomllib.load(f)

            dependencies = data.get("project", {}).get("dependencies", [])
            if dependencies:
                return dependencies

        requirements_txt = Path.cwd() / "requirements.txt"
        if requirements_txt.exists():
            requirements = [
                line.strip()
                for line in requirements_txt.read_text().splitlines()
                if line.strip() and not line.strip().startswith("#")
            ]
            if requirements:
                return requirements

        return [
            self._installed_package_requirement("apache-airflow"),
            self._installed_package_requirement("psycopg2-binary"),
            self._installed_package_requirement("rocrate"),
        ]

    def _installed_package_requirement(self, package_name: str) -> str:
        """Pin a package version when it is installed locally."""
        try:
            return f"{package_name}=={importlib_metadata.version(package_name)}"
        except importlib_metadata.PackageNotFoundError:
            return package_name

    def _airflow_version(self, metadata: Optional[Dict[str, Any]]) -> str:
        """Use the captured Airflow version when available."""
        if metadata and metadata.get("airflow_version"):
            return str(metadata["airflow_version"])

        try:
            return importlib_metadata.version("apache-airflow")
        except importlib_metadata.PackageNotFoundError:
            return "unknown"

    def _capture_worker_environment(self, environment_dir: Path):
        """Capture worker details when a Docker Compose worker is running."""
        docker_compose = Path.cwd() / "docker-compose.yaml"
        if not docker_compose.exists():
            return

        if not shutil.which("docker"):
            return

        services = self._run_command(
            ["docker", "compose", "ps", "--services", "--status", "running"],
            timeout=5,
        )
        if services["returncode"] != 0:
            return

        worker_service = self._select_worker_service(services["stdout"].splitlines())
        if not worker_service:
            return

        captures = {
            "worker-python-version.txt": ["python", "--version"],
            "worker-pip-freeze.txt": ["python", "-m", "pip", "freeze"],
            "worker-os-release.txt": ["cat", "/etc/os-release"],
        }
        for filename, worker_command in captures.items():
            command = ["docker", "compose", "exec", "-T", worker_service, *worker_command]
            completed = self._run_command(command, timeout=30)
            if completed["returncode"] == 0:
                (environment_dir / filename).write_text(completed["stdout"])

    def _select_worker_service(self, services: List[str]) -> Optional[str]:
        cleaned = [service.strip() for service in services if service.strip()]
        for preferred in ("airflow-worker", "worker"):
            if preferred in cleaned:
                return preferred
        for service in cleaned:
            if "worker" in service:
                return service
        return None

    def _run_command(self, command: List[str], timeout: int) -> Dict[str, Any]:
        """Run a command and return its output."""
        try:
            completed = subprocess.run(
                command,
                cwd=Path.cwd(),
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
            return {
                "returncode": completed.returncode,
                "stdout": completed.stdout,
                "stderr": completed.stderr,
            }
        except Exception as e:
            return {
                "returncode": -1,
                "stdout": "",
                "stderr": str(e),
            }
