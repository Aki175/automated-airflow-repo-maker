from __future__ import annotations

import csv
import hashlib
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator


def _project_root() -> Path:
    return Path(os.getenv("AIRFLOW_ROCRATE_PROJECT_ROOT", "/opt/airflow"))


def prepare_run_context(**context: Any) -> dict[str, Any]:
    logical_date = context["logical_date"].isoformat()
    csv_path = Path(
        os.getenv(
            "AIRFLOW_ROCRATE_SAMPLE_CSV",
            str(_project_root() / "data" / "sample_measurements.csv"),
        )
    )
    manual_pdf = os.getenv(
        "AIRFLOW_ROCRATE_MANUAL_PDF",
        str(_project_root() / "data" / "manual_protocol.pdf"),
    )
    return {
        "experiment_id": f"edge-case-parallel-{logical_date}",
        "csv_path": str(csv_path),
        "manual_pdf_path": manual_pdf,
        "note": "CSV is read by the DAG; PDF is an optional manual artifact for --include-data.",
    }


def read_csv_and_summarize(**context: Any) -> dict[str, Any]:
    run_context = context["ti"].xcom_pull(task_ids="prepare_run_context") or {}
    csv_path = Path(run_context["csv_path"])

    with csv_path.open(newline="") as csv_file:
        rows = list(csv.DictReader(csv_file))

    values = [float(row["value"]) for row in rows]
    digest = hashlib.sha256(csv_path.read_bytes()).hexdigest()
    by_group: dict[str, list[float]] = {}
    for row in rows:
        by_group.setdefault(row["group"], []).append(float(row["value"]))

    group_means = {
        group: round(sum(group_values) / len(group_values), 3)
        for group, group_values in by_group.items()
    }
    time.sleep(8)

    return {
        "provenance_type": "local_data_file",
        "name": csv_path.name,
        "path": str(csv_path),
        "sha256": digest,
        "size_bytes": csv_path.stat().st_size,
        "row_count": len(rows),
        "value_mean": round(sum(values) / len(values), 3),
        "group_means": group_means,
    }


def simulate_model_training(**context: Any) -> dict[str, Any]:
    run_context = context["ti"].xcom_pull(task_ids="prepare_run_context") or {}
    time.sleep(10)
    return {
        "experiment_id": run_context.get("experiment_id"),
        "operator_case": "PythonOperator",
        "model_type": "demo-threshold-model",
        "parameters": {
            "threshold": 15.0,
            "normalization": "none",
        },
        "metric": {
            "accuracy": 0.8,
            "explain": "Synthetic metric for reproducibility capture testing.",
        },
    }


def record_manual_pdf_reference(**context: Any) -> dict[str, Any]:
    run_context = context["ti"].xcom_pull(task_ids="prepare_run_context") or {}
    pdf_path = Path(run_context["manual_pdf_path"])
    payload = {
        "provenance_type": "manual_artifact_reference",
        "name": pdf_path.name,
        "path": str(pdf_path),
        "description": "Optional protocol PDF that can be included with --include-data.",
        "included_by_default": False,
    }
    if pdf_path.exists():
        payload["sha256"] = hashlib.sha256(pdf_path.read_bytes()).hexdigest()
        payload["size_bytes"] = pdf_path.stat().st_size
    else:
        payload["status"] = "not_present_during_dag_run"
    time.sleep(6)
    return payload


def combine_parallel_results(**context: Any) -> dict[str, Any]:
    task_instance = context["ti"]
    csv_summary = task_instance.xcom_pull(task_ids="read_csv_and_summarize") or {}
    model_summary = task_instance.xcom_pull(task_ids="simulate_model_training") or {}
    pdf_reference = task_instance.xcom_pull(task_ids="record_manual_pdf_reference") or {}

    return {
        "csv_rows": csv_summary.get("row_count"),
        "csv_sha256": csv_summary.get("sha256"),
        "model_type": model_summary.get("model_type"),
        "manual_pdf_status": pdf_reference.get("status", "present"),
        "parallel_inputs": [
            "read_csv_and_summarize",
            "simulate_model_training",
            "record_manual_pdf_reference",
            "bash_parallel_probe",
        ],
    }


with DAG(
    dag_id="edge_case_parallel_data_dag",
    description="Parallel branches, mixed operators, CSV XCom provenance, and optional manual data",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["reproducibility", "parallel", "data", "edge-case"],
) as dag:
    start = EmptyOperator(task_id="start")

    prepare = PythonOperator(
        task_id="prepare_run_context",
        python_callable=prepare_run_context,
    )

    read_csv = PythonOperator(
        task_id="read_csv_and_summarize",
        python_callable=read_csv_and_summarize,
    )

    train_model = PythonOperator(
        task_id="simulate_model_training",
        python_callable=simulate_model_training,
    )

    pdf_reference = PythonOperator(
        task_id="record_manual_pdf_reference",
        python_callable=record_manual_pdf_reference,
    )

    bash_probe = BashOperator(
        task_id="bash_parallel_probe",
        bash_command="sleep 7 && echo 'parallel bash branch finished'",
    )

    join = PythonOperator(
        task_id="combine_parallel_results",
        python_callable=combine_parallel_results,
    )

    finish = EmptyOperator(task_id="finish")

    start >> prepare
    prepare >> [read_csv, train_model, pdf_reference, bash_probe] >> join >> finish
