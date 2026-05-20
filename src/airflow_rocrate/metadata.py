"""
Read run information from the Airflow metadata database.
"""

from datetime import datetime
from importlib import metadata as importlib_metadata
from typing import Dict, List, Any, Optional
import json
from pathlib import Path

from sqlalchemy import MetaData, Table, create_engine, inspect, select
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

try:
    from airflow.models import DagRun, TaskInstance
except ImportError:
    raise ImportError("Apache Airflow must be installed to use this package")


class AirflowMetadataCapture:
    """Read DAG run metadata from Airflow."""

    def __init__(self, db_url: str):
        """Connect to the Airflow metadata database."""
        self.db_url = db_url
        self.engine = create_engine(db_url)
        session_factory = sessionmaker(bind=self.engine)
        self.session: Session = session_factory()

    def get_dag_info(self, dag_id: str) -> Dict[str, Any]:
        """Get basic information about a DAG."""
        try:
            from airflow.models import DagModel

            dag_model = self.session.query(DagModel).filter(
                DagModel.dag_id == dag_id
            ).first()

            if not dag_model:
                return {}

            owner = None
            if hasattr(dag_model, 'owner'):
                owner = dag_model.owner
            elif hasattr(dag_model, 'owner_id'):
                owner = dag_model.owner_id

            created = None
            if hasattr(dag_model, 'created_date'):
                created = dag_model.created_date.isoformat()
            elif hasattr(dag_model, 'created_at'):
                created = dag_model.created_at.isoformat()

            last_updated = None
            if hasattr(dag_model, 'last_updated'):
                last_updated = dag_model.last_updated.isoformat()
            elif hasattr(dag_model, 'last_parsed'):
                last_updated = dag_model.last_parsed.isoformat()

            return {
                "dag_id": dag_model.dag_id,
                "description": dag_model.description or "N/A",
                "owner": owner or "N/A",
                "created_at": created,
                "last_updated": last_updated,
                "tags": [tag.name if hasattr(tag, 'name') else str(tag) for tag in (getattr(dag_model, 'tags', []) or [])],
            }
        except Exception as e:
            print(f"Error fetching DAG info: {e}")
            return {}

    def get_dag_runs(
        self,
        dag_id: str,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Get recent runs for a DAG."""
        dag_runs = self.session.query(DagRun).filter(
            DagRun.dag_id == dag_id
        ).order_by(DagRun.logical_date.desc()).limit(limit).all()

        return [
            {
                "run_id": run.run_id,
                "logical_date": run.logical_date.isoformat() if run.logical_date else None,
                "start_date": run.start_date.isoformat() if run.start_date else None,
                "end_date": run.end_date.isoformat() if run.end_date else None,
                "state": run.state,
                "duration_seconds": (run.end_date - run.start_date).total_seconds() if run.start_date and run.end_date else None,
            }
            for run in dag_runs
        ]

    def get_task_instances(
        self, 
        dag_id: str, 
        run_id: str
    ) -> List[Dict[str, Any]]:
        """Get the task instances for one DAG run."""
        try:
            task_instances = self.session.query(TaskInstance).filter(
                TaskInstance.dag_id == dag_id,
                TaskInstance.run_id == run_id,
            ).all()
            
            return [
                {
                    "task_id": ti.task_id,
                    "state": ti.state,
                    "start_date": ti.start_date.isoformat() if ti.start_date else None,
                    "end_date": ti.end_date.isoformat() if ti.end_date else None,
                    "duration_seconds": (ti.end_date - ti.start_date).total_seconds() if ti.start_date and ti.end_date else None,
                    "try_number": ti.try_number,
                    "max_tries": ti.max_tries,
                    "operator": getattr(ti, "operator", None),
                    "hostname": getattr(ti, "hostname", None),
                    "unixname": getattr(ti, "unixname", None),
                    "queue": getattr(ti, "queue", None),
                    "pool": getattr(ti, "pool", None),
                }
                for ti in task_instances
            ]
        except Exception as e:
            print(f"Error fetching task instances: {e}")
            return []

    def get_task_logs(
        self,
        dag_id: str,
        task_id: str, 
        run_id: str,
        try_number: int = 1
    ) -> Optional[str]:
        """Get log messages for one task."""
        try:
            from airflow.models import Log
            
            logs = self.session.query(Log).filter(
                Log.dag_id == dag_id,
                Log.task_id == task_id,
            ).all()
            
            if not logs:
                return None
            
            filtered = [log for log in logs if hasattr(log, 'run_id') and log.run_id == run_id]
            if not filtered:
                filtered = logs[:10]

            return "\n".join([log.message for log in filtered if hasattr(log, 'message') and log.message])
        except Exception as e:
            print(f"Error fetching logs: {e}")
            return None

    def get_xcom_data(
        self,
        dag_id: str,
        run_id: str
    ) -> Dict[str, Any]:
        """Get XCom values shared between tasks."""
        try:
            inspector = inspect(self.engine)
            if "xcom" not in inspector.get_table_names():
                return {}

            metadata = MetaData()
            xcom = Table("xcom", metadata, autoload_with=self.engine)
            columns = xcom.c

            statement = select(xcom)
            if "dag_id" in columns:
                statement = statement.where(columns.dag_id == dag_id)
            if "run_id" in columns:
                statement = statement.where(columns.run_id == run_id)

            xcoms = self.session.execute(statement).mappings().all()
            result = {}
            for xcom in xcoms:
                task_id = xcom.get("task_id", "unknown")
                key = xcom.get("key", "value")
                value_key = f"{task_id}_{key}"

                if "value" in xcom:
                    result[value_key] = self._serialize_xcom_value(xcom["value"])
            
            return result
        except Exception as e:
            print(f"Error fetching XCom data: {e}")
            return {}

    def get_dag_source_code(self, dag_id: str) -> Optional[str]:
        """Get the DAG Python source from the database when Airflow stores it."""
        try:
            try:
                from airflow.models import DagCode
            except ImportError:
                try:
                    from airflow.models.dag import DagCode
                except ImportError:
                    print("DagCode model not found in this Airflow version")
                    return None
            
            dag_code = self.session.query(DagCode).filter(
                DagCode.dag_id == dag_id
            ).first()
            
            if dag_code and hasattr(dag_code, 'code'):
                return dag_code.code
            
            return None
        except Exception as e:
            print(f"Error fetching DAG source code: {e}")
            return None

    def _serialize_xcom_value(self, value: Any) -> Any:
        """Convert XCom values into something JSON can store."""
        if value is None or isinstance(value, (str, int, float, bool, list, dict)):
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return value
            return value

        if isinstance(value, bytes):
            try:
                decoded = value.decode("utf-8")
            except UnicodeDecodeError:
                return value.hex()
            try:
                return json.loads(decoded)
            except json.JSONDecodeError:
                return decoded

        return str(value)

    def get_airflow_version(self) -> str:
        """Get the Airflow version seen by this process."""
        try:
            return importlib_metadata.version("apache-airflow")
        except importlib_metadata.PackageNotFoundError:
            return "unknown"

    def safe_db_url(self) -> str:
        """Return the database URL without the password."""
        try:
            return str(make_url(self.db_url).render_as_string(hide_password=True))
        except Exception:
            return "<unavailable>"

    def capture_full_execution(
        self,
        dag_id: str,
        run_id: str,
        output_dir: Path
    ) -> Dict[str, Any]:
        """Capture the metadata needed to describe one DAG run."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        metadata = {
            "capture_timestamp": datetime.now().isoformat(),
            "dag_id": dag_id,
            "run_id": run_id,
            "airflow_version": self.get_airflow_version(),
            "metadata_database": {
                "url": self.safe_db_url(),
                "note": "Airflow run metadata was captured by querying this metadata database.",
            },
            "dag_info": self.get_dag_info(dag_id),
            "task_instances": self.get_task_instances(dag_id, run_id),
            "xcom_data": self.get_xcom_data(dag_id, run_id),
        }
        
        metadata_file = output_dir / "metadata.json"
        with open(metadata_file, "w") as f:
            json.dump(metadata, f, indent=2)
        
        source_code = self.get_dag_source_code(dag_id)
        if source_code:
            code_file = output_dir / f"{dag_id}_source.py"
            with open(code_file, "w") as f:
                f.write(source_code)
        
        try:
            for task_instance in metadata["task_instances"]:
                task_id = task_instance["task_id"]
                logs = self.get_task_logs(dag_id, task_id, run_id, task_instance.get("try_number", 1))
                if logs:
                    log_file = output_dir / f"task_logs_{task_id}.log"
                    with open(log_file, "w") as f:
                        f.write(logs)
        except Exception as e:
            print(f"Warning: Could not save task logs: {e}")
        
        return metadata

    def close(self):
        """Close the database session."""
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
