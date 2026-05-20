"""
Branching DAG for reproducibility capture.

This DAG intentionally creates skipped tasks so the package can show that a DAG
run records both executed and skipped branches.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule


def choose_branch(**context: Any) -> str:
    selected = os.getenv("AIRFLOW_ROCRATE_BRANCH", "path_a")
    if selected not in {"path_a", "path_b"}:
        selected = "path_a"
    return selected


def branch_payload(branch_name: str, **context: Any) -> dict[str, Any]:
    return {
        "branch": branch_name,
        "logical_date": context["logical_date"].isoformat(),
        "message": f"{branch_name} was selected and executed.",
    }


def summarize_branch_run(**context: Any) -> dict[str, Any]:
    task_instance = context["ti"]
    path_a = task_instance.xcom_pull(task_ids="path_a")
    path_b = task_instance.xcom_pull(task_ids="path_b")
    selected_payload = path_a or path_b or {}
    return {
        "selected_branch": selected_payload.get("branch"),
        "path_a_executed": path_a is not None,
        "path_b_executed": path_b is not None,
        "expected_skipped_task": "path_b" if path_a else "path_a",
    }


with DAG(
    dag_id="edge_case_branching_dag",
    description="BranchPythonOperator DAG with one executed branch and one skipped branch",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["reproducibility", "branching", "skipped", "edge-case"],
) as dag:
    start = EmptyOperator(task_id="start")

    choose = BranchPythonOperator(
        task_id="choose_branch",
        python_callable=choose_branch,
    )

    path_a = PythonOperator(
        task_id="path_a",
        python_callable=branch_payload,
        op_kwargs={"branch_name": "path_a"},
    )

    path_b = PythonOperator(
        task_id="path_b",
        python_callable=branch_payload,
        op_kwargs={"branch_name": "path_b"},
    )

    summarize = PythonOperator(
        task_id="summarize_branch_run",
        python_callable=summarize_branch_run,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    finish = EmptyOperator(task_id="finish")

    start >> choose >> [path_a, path_b] >> summarize >> finish
