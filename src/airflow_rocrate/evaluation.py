from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional


CATEGORY_SCORES = {
    "automatic": 1.0,
    "deployment_specific": 0.5,
    "manual": 0.0,
    "unsupported": 0.0,
}


@dataclass(frozen=True)
class CoverageItem:
    name: str
    category: str
    captured: bool
    score: float
    reason: str

    def as_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "category": self.category,
            "captured": self.captured,
            "score": self.score,
            "reason": self.reason,
        }


def build_coverage_report(
    package_dir: Path,
    metadata: Dict[str, Any],
    data_files: Optional[Iterable[Path]] = None,
) -> Dict[str, Any]:
    package_dir = Path(package_dir)
    planned_data_files = list(data_files or [])
    dag_run = metadata.get("dag_run") or {}

    items = [
        _coverage_item(
            "dag_run_metadata",
            "automatic",
            _has_text(dag_run.get("run_id"))
            and _has_text(dag_run.get("state"))
            and _has_text(dag_run.get("start_date")),
            "Run ID, state and timing were captured from the Airflow DagRun record.",
        ),
        _coverage_item(
            "task_instance_metadata",
            "automatic",
            bool(metadata.get("task_instances")),
            "Task states, operators and timings were captured from Airflow task instances.",
        ),
        _coverage_item(
            "xcom_metadata",
            "automatic",
            "xcom_data" in metadata,
            "The XCom table was inspected for the selected DAG run.",
        ),
        _coverage_item(
            "airflow_version",
            "automatic",
            _has_text(metadata.get("airflow_version")) and metadata.get("airflow_version") != "unknown",
            "The Airflow package version was captured when available.",
        ),
        _coverage_item(
            "ro_crate_metadata",
            "automatic",
            (package_dir / "ro-crate-metadata.json").exists(),
            "The package contains the RO-Crate JSON-LD metadata file.",
        ),
        _coverage_item(
            "dag_source_file",
            "automatic",
            _has_any_file(package_dir / "workflow" / "dags"),
            "A DAG source file was added to the package.",
        ),
        _coverage_item(
            "task_logs",
            "deployment_specific",
            _has_any_file(package_dir / "logs" / "airflow-task-logs"),
            "Task logs were copied when the local deployment exposed filesystem logs.",
        ),
        _coverage_item(
            "worker_environment",
            "deployment_specific",
            _has_any_worker_file(package_dir / "environment"),
            "Docker worker runtime evidence was captured when a running worker was available.",
        ),
    ]

    if planned_data_files:
        items.append(
            _coverage_item(
                "user_included_data",
                "manual",
                _has_any_file(package_dir / "data"),
                "Data files were included through explicit user input, not automatic discovery.",
            )
        )

    return coverage_report(items)


def coverage_report(items: Iterable[CoverageItem]) -> Dict[str, Any]:
    item_list = list(items)
    total_items = len(item_list)
    total_score = sum(item.score for item in item_list)
    score = total_score / total_items if total_items else 0.0

    return {
        "formula": "Coverage = sum(s_i) / n",
        "score": round(score, 4),
        "score_percent": round(score * 100, 2),
        "total_score": round(total_score, 4),
        "total_items": total_items,
        "items": [item.as_dict() for item in item_list],
        "score_meaning": {
            "automatic": 1.0,
            "deployment_specific": 0.5,
            "manual": 0.0,
            "unsupported": 0.0,
        },
    }


def _coverage_item(name: str, category: str, captured: bool, reason: str) -> CoverageItem:
    if category not in CATEGORY_SCORES:
        raise ValueError(f"Unknown coverage category: {category}")
    return CoverageItem(
        name=name,
        category=category,
        captured=captured,
        score=CATEGORY_SCORES[category] if captured else 0.0,
        reason=reason,
    )


def _has_text(value: Any) -> bool:
    return value is not None and str(value).strip() != ""


def _has_any_file(path: Path) -> bool:
    return path.exists() and any(child.is_file() for child in path.rglob("*"))


def _has_any_worker_file(environment_dir: Path) -> bool:
    worker_files = (
        "worker-python-version.txt",
        "worker-pip-freeze.txt",
        "worker-os-release.txt",
    )
    return any((environment_dir / filename).exists() for filename in worker_files)
