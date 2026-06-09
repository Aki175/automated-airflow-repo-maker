from __future__ import annotations

import os
from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


def d1_prepare_parameters(**context: Any) -> dict[str, Any]:
    logical_date = context["logical_date"].isoformat()
    external_uri = os.getenv(
        "AIRFLOW_ROCRATE_EXTERNAL_URI",
        "s3://airflow-rocrate-demo/inputs/example.csv",
    )

    return {
        "experiment_id": f"airflow-repro-demo-{logical_date}",
        "parameters": {
            "threshold": 0.8,
            "sample_size": 10,
            "mode": "demo",
        },
        "expected_external_input": external_uri,
    }


def d2_record_external_reference(**context: Any) -> dict[str, Any]:
    task_instance = context["ti"]
    run_context = task_instance.xcom_pull(task_ids="d1_prepare_parameters") or {}

    service = os.getenv("AIRFLOW_ROCRATE_EXTERNAL_SERVICE", "aws_s3")
    service_name = os.getenv("AIRFLOW_ROCRATE_EXTERNAL_SERVICE_NAME", "Amazon S3")
    service_type = os.getenv("AIRFLOW_ROCRATE_EXTERNAL_SERVICE_TYPE", "Object storage")
    uri = os.getenv(
        "AIRFLOW_ROCRATE_EXTERNAL_URI",
        run_context.get("expected_external_input", "s3://airflow-rocrate-demo/inputs/example.csv"),
    )

    return {
        "provenance_type": "external_data_reference",
        "service": service,
        "service_name": service_name,
        "service_type": service_type,
        "uri": uri,
        "identifier": uri,
        "access_method": os.getenv("AIRFLOW_ROCRATE_ACCESS_METHOD", "reference_only"),
        "bucket": os.getenv("AIRFLOW_ROCRATE_BUCKET", "airflow-rocrate-demo"),
        "key": os.getenv("AIRFLOW_ROCRATE_OBJECT_KEY", "inputs/example.csv"),
        "region": os.getenv("AIRFLOW_ROCRATE_REGION", "eu-west-1"),
        "upstream_experiment_id": run_context.get("experiment_id"),
        "note": "External reference captured as provenance; no external service call is required.",
    }


with DAG(
    dag_id="complex_repro_dag",
    description="Three step DAG with XCom handoff and service neutral external provenance",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["reproducibility", "xcom", "external-service", "demo"],
) as dag:
    d1 = PythonOperator(
        task_id="d1_prepare_parameters",
        python_callable=d1_prepare_parameters,
    )

    d2 = PythonOperator(
        task_id="d2_record_external_reference",
        python_callable=d2_record_external_reference,
    )

    d3 = BashOperator(
        task_id="d3_report_summary",
        bash_command="""
        echo "Complex reproducibility DAG complete"
        echo "Experiment: {{ ti.xcom_pull(task_ids='d1_prepare_parameters')['experiment_id'] }}"
        echo "External input: {{ ti.xcom_pull(task_ids='d2_record_external_reference')['uri'] }}"
        echo "Service: {{ ti.xcom_pull(task_ids='d2_record_external_reference')['service_name'] }}"
        """,
    )

    d1 >> d2 >> d3
