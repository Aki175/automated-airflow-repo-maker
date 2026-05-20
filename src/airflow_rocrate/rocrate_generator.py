"""
Build RO-Crate metadata for captured Airflow DAG runs.
"""

from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
import json

from rocrate.rocrate import ROCrate
from rocrate.model.contextentity import ContextEntity


class RocrateCrateGenerator:
    """Turn captured Airflow metadata into an RO-Crate."""

    def __init__(self, output_dir: Path):
        """Set up the folder where the crate will be written."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def create_rocrate_from_metadata(
        self,
        dag_id: str,
        run_id: str,
        metadata: Dict[str, Any],
        dag_files: List[Path] = None,
        data_files: List[Path] = None,
    ) -> Path:
        """Create an RO-Crate from one captured DAG run."""
        crate = ROCrate()

        dag_info = metadata.get("dag_info", {})
        crate.root_dataset["name"] = f"Airflow DAG Execution: {dag_id}"
        crate.root_dataset["description"] = (
            f"Full reproducibility package for Airflow DAG '{dag_id}' "
            f"execution {run_id}"
        )
        crate.root_dataset["datePublished"] = metadata["capture_timestamp"]
        crate.root_dataset["keywords"] = [
            "airflow",
            "workflow",
            "reproducibility",
            "provenance",
            dag_id,
        ]

        person_id = self._person_id(dag_info.get("owner", "Unknown"))
        airflow_id = "#software_apache_airflow"

        author = ContextEntity(
            crate,
            person_id,
            properties={
                "@type": "Person",
                "name": dag_info.get("owner", "Unknown"),
            },
        )
        airflow = ContextEntity(
            crate,
            airflow_id,
            properties={
                "@type": "SoftwareApplication",
                "name": "Apache Airflow",
                "version": metadata.get("airflow_version", "unknown"),
                "url": "https://airflow.apache.org/",
            },
        )
        crate.add(author, airflow)
        crate.root_dataset["author"] = {"@id": person_id}

        self._add_workflow_definition(crate, dag_id, run_id, metadata, person_id)

        self._add_execution_activity(crate, dag_id, run_id, metadata, airflow_id)

        self._add_task_information(crate, metadata.get("task_instances", []))

        self._add_xcom_information(crate, metadata.get("xcom_data", {}))

        if dag_files:
            for dag_file in dag_files:
                self._add_file_to_crate(crate, dag_file, "dag_source")

        if data_files:
            for data_file in data_files:
                self._add_file_to_crate(crate, data_file, "data")

        rocrate_path = self.output_dir / f"rocrate_{self._safe_identifier(dag_id)}_{self._safe_identifier(run_id)}"
        crate.write(str(rocrate_path))

        return rocrate_path

    def _add_workflow_definition(
        self,
        crate: ROCrate,
        dag_id: str,
        run_id: str,
        metadata: Dict[str, Any],
        person_id: str,
    ):
        """Add the DAG as the workflow being described."""
        dag_info = metadata.get("dag_info", {})

        workflow_id = f"#{dag_id}"
        workflow_properties = {
            "@type": "SoftwareSourceCode",
            "name": dag_id,
            "description": dag_info.get("description", f"Airflow DAG: {dag_id}"),
            "author": {"@id": person_id},
            "programmingLanguage": {"@id": "https://www.python.org/"},
        }

        if dag_info.get("created_at"):
            workflow_properties["dateCreated"] = dag_info["created_at"]
        if dag_info.get("last_updated"):
            workflow_properties["dateModified"] = dag_info["last_updated"]

        if dag_info.get("tags"):
            workflow_properties["keywords"] = dag_info["tags"]

        workflow = ContextEntity(crate, workflow_id, properties=workflow_properties)
        crate.add(workflow)
        self._append_root_reference(crate, "mentions", workflow_id)

    def _add_execution_activity(
        self,
        crate: ROCrate,
        dag_id: str,
        run_id: str,
        metadata: Dict[str, Any],
        airflow_id: str,
    ):
        """Add the activity that represents the DAG run."""
        capture_time = metadata["capture_timestamp"]

        activity_id = f"#execution_{self._safe_identifier(run_id)}"
        activity = ContextEntity(
            crate,
            activity_id,
            properties={
                "@type": "Action",
                "name": f"Execution of {dag_id}",
                "description": f"Airflow DAG execution {run_id}",
                "object": {"@id": f"#{dag_id}"},
                "startTime": capture_time,
                "endTime": capture_time,
                "agent": {"@id": airflow_id},
            },
        )

        crate.add(activity)
        crate.root_dataset["result"] = {"@id": activity_id}

    def _add_task_information(self, crate: ROCrate, task_instances: List[Dict[str, Any]]):
        """Add task run details and detect overlapping task times."""
        task_entities = []
        for idx, task in enumerate(task_instances):
            task_id = f"#task_{idx}_{self._safe_identifier(task['task_id'])}"
            duration = task.get("duration_seconds") or 0
            task_properties = {
                "@type": "HowToStep",
                "name": task["task_id"],
                "description": f"Task: {task['task_id']} | State: {task['state']}",
                "position": idx + 1,
                "duration": f"PT{int(duration)}S",
            }

            if task.get("start_date"):
                task_properties["startTime"] = task["start_date"]
            if task.get("end_date"):
                task_properties["endTime"] = task["end_date"]
            if task.get("state"):
                task_properties["actionStatus"] = task["state"]
            if task.get("operator"):
                task_properties["airflowOperator"] = task["operator"]
            if task.get("try_number") is not None:
                task_properties["airflowTryNumber"] = task["try_number"]
            if task.get("hostname"):
                task_properties["airflowHostname"] = task["hostname"]

            task_entity = ContextEntity(
                crate,
                task_id,
                properties=task_properties,
            )
            crate.add(task_entity)
            task_entities.append((task_id, task_entity, task))

            self._append_root_reference(crate, "step", task_id)

        self._add_parallel_execution_information(crate, task_entities)

    def _add_parallel_execution_information(
        self,
        crate: ROCrate,
        task_entities: List[tuple[str, ContextEntity, Dict[str, Any]]],
    ):
        """Record tasks that ran at the same time."""
        parallel_overlaps = []

        for left_index, (left_id, left_entity, left_task) in enumerate(task_entities):
            left_start = self._parse_datetime(left_task.get("start_date"))
            left_end = self._parse_datetime(left_task.get("end_date"))
            if not left_start or not left_end:
                continue

            for right_index in range(left_index + 1, len(task_entities)):
                right_id, right_entity, right_task = task_entities[right_index]
                right_start = self._parse_datetime(right_task.get("start_date"))
                right_end = self._parse_datetime(right_task.get("end_date"))
                if not right_start or not right_end:
                    continue

                overlap_start = max(left_start, right_start)
                overlap_end = min(left_end, right_end)
                if overlap_start >= overlap_end:
                    continue

                overlap_seconds = int((overlap_end - overlap_start).total_seconds())
                overlap_id = (
                    f"#parallel_overlap_{left_index}_{right_index}_"
                    f"{self._safe_identifier(left_task['task_id'])}_"
                    f"{self._safe_identifier(right_task['task_id'])}"
                )
                overlap_entity = ContextEntity(
                    crate,
                    overlap_id,
                    properties={
                        "@type": "Action",
                        "additionalType": "airflow:ParallelTaskOverlap",
                        "name": (
                            f"Parallel execution overlap: "
                            f"{left_task['task_id']} and {right_task['task_id']}"
                        ),
                        "description": (
                            "Two Airflow task instances from the same DAG run had "
                            "overlapping start/end timestamps."
                        ),
                        "object": [{"@id": left_id}, {"@id": right_id}],
                        "startTime": overlap_start.isoformat(),
                        "endTime": overlap_end.isoformat(),
                        "duration": f"PT{overlap_seconds}S",
                        "measurementTechnique": "Airflow task instance timestamp overlap",
                    },
                )
                crate.add(overlap_entity)
                parallel_overlaps.append({"@id": overlap_id})
                self._append_entity_reference(left_entity, "concurrentWith", right_id)
                self._append_entity_reference(right_entity, "concurrentWith", left_id)

        if parallel_overlaps:
            crate.root_dataset["airflowParallelOverlap"] = parallel_overlaps
            for overlap in parallel_overlaps:
                self._append_root_reference(crate, "mentions", overlap["@id"])

    def _add_xcom_information(self, crate: ROCrate, xcom_data: Dict[str, Any]):
        """Add XCom values produced during the run."""
        if not xcom_data:
            return

        for key, value in xcom_data.items():
            structured_value = self._structured_xcom_value(value)
            xcom_id = f"#xcom_{self._safe_identifier(key)}"
            xcom_properties = {
                "@type": "PropertyValue",
                "name": key,
                "description": f"Airflow XCom value captured for {key}",
                "propertyID": "airflow:xcom",
                "value": self._json_safe_value(structured_value),
            }

            external_reference_id = self._add_external_reference_from_xcom(crate, key, structured_value)
            if external_reference_id:
                xcom_properties["about"] = {"@id": external_reference_id}

            xcom_entity = ContextEntity(
                crate,
                xcom_id,
                properties=xcom_properties,
            )
            crate.add(xcom_entity)
            self._append_root_reference(crate, "mentions", xcom_id)

    def _add_external_reference_from_xcom(
        self,
        crate: ROCrate,
        key: str,
        value: Any,
    ) -> Optional[str]:
        """Turn structured XCom provenance into linked crate entities."""
        if not isinstance(value, dict):
            return None

        provenance_type = value.get("provenance_type") or value.get("provenanceType")
        service = value.get("service") or value.get("provider") or value.get("system")
        service_name = value.get("service_name") or value.get("serviceName")
        service_type = value.get("service_type") or value.get("serviceType")
        identifier = value.get("identifier")
        uri = (
            value.get("uri")
            or value.get("content_url")
            or value.get("contentUrl")
            or value.get("url")
            or value.get("path")
            or value.get("file_path")
            or value.get("filePath")
            or identifier
        )

        is_external_reference = (
            provenance_type == "external_data_reference"
            or str(provenance_type).endswith("_reference")
            or value.get("is_external_reference") is True
            or bool(uri and (service or service_name or provenance_type))
        )
        if not is_external_reference:
            return None

        service_slug = service or service_name or "external_service"
        service_id = f"#service_{self._safe_identifier(service_slug)}"
        reference_id = f"#external_reference_{self._safe_identifier(uri or identifier or key)}"
        service_display_name = service_name or str(service_slug).replace("_", " ").title()
        service_url = value.get("service_url") or value.get("serviceUrl") or value.get("endpoint")

        service_entity = ContextEntity(
            crate,
            service_id,
            properties=self._without_empty_values({
                "@type": "Service",
                "name": service_display_name,
                "serviceType": service_type or "External data service",
                "identifier": service,
                "url": service_url,
            }),
        )

        reference_properties = self._without_empty_values({
            "@type": "DataDownload",
            "name": value.get("name") or f"External data reference: {uri or identifier or key}",
            "description": value.get("description")
            or "External data/service reference captured by the Airflow DAG run.",
            "contentUrl": uri,
            "identifier": identifier or uri,
            "provider": {"@id": service_id},
            "checksum": self._first_present(value, "checksum", "sha256", "md5"),
            "mediaType": value.get("media_type") or value.get("mediaType"),
        })
        additional_properties = self._add_xcom_additional_property_entities(
            crate,
            reference_id,
            value,
        )
        if additional_properties:
            reference_properties["additionalProperty"] = additional_properties

        reference_entity = ContextEntity(
            crate,
            reference_id,
            properties=reference_properties,
        )
        crate.add(service_entity, reference_entity)
        self._append_root_reference(crate, "mentions", reference_id)
        return reference_id

    def _add_file_to_crate(self, crate: ROCrate, file_path: Path, file_type: str = "data"):
        """Copy a file into the crate."""
        try:
            if file_type == "dag_source":
                dest_path = f"workflow/dags/{file_path.name}"
            elif file_type == "data":
                dest_path = f"data/{file_path.name}"
            else:
                dest_path = file_path.name
            crate.add_file(str(file_path), dest_path=dest_path, fetch_remote=False)
        except Exception as e:
            print(f"Warning: Could not add file {file_path} to crate: {e}")

    def _person_id(self, owner: str) -> str:
        """Create a stable local person ID."""
        owner = owner or "Unknown"
        return f"#person_{self._safe_identifier(owner)}"

    def _safe_identifier(self, value: str) -> str:
        """Convert a value into a simple ID fragment."""
        return "".join(ch if ch.isalnum() else "_" for ch in str(value)).strip("_") or "unknown"

    def _json_safe_value(self, value: Any) -> Any:
        """Convert a value into something JSON can store."""
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value

        try:
            return json.dumps(value, sort_keys=True)
        except TypeError:
            return str(value)

    def _structured_xcom_value(self, value: Any) -> Any:
        """Decode XCom strings that contain JSON."""
        if not isinstance(value, str):
            return value

        stripped = value.strip()
        if not stripped or stripped[0] not in "[{":
            return value

        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            return value

    def _parse_datetime(self, value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None

        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None

    def _append_entity_reference(
        self,
        entity: ContextEntity,
        property_name: str,
        target_id: str,
    ):
        """Add a linked entity ID to an entity property."""
        existing = entity.get(property_name)
        reference = {"@id": target_id}
        if existing is None:
            entity[property_name] = [reference]
        elif isinstance(existing, list):
            existing.append(reference)
        else:
            entity[property_name] = [existing, reference]

    def _append_root_reference(
        self,
        crate: ROCrate,
        property_name: str,
        target_id: str,
    ):
        """Add a linked entity ID to the root dataset."""
        existing = crate.root_dataset.get(property_name)
        reference = {"@id": target_id}

        if existing is None:
            references = [reference]
        elif isinstance(existing, list):
            references = [self._entity_reference(item) for item in existing]
            if reference in references:
                return
            references.append(reference)
        else:
            references = [self._entity_reference(existing)]
            if reference not in references:
                references.append(reference)

        crate.root_dataset[property_name] = references

    def _entity_reference(self, value: Any) -> Dict[str, str]:
        """Convert different reference shapes into an @id dictionary."""
        if isinstance(value, dict) and "@id" in value:
            return {"@id": str(value["@id"])}

        entity_id = getattr(value, "id", None)
        if entity_id is not None:
            return {"@id": str(entity_id)}

        return {"@id": str(value)}

    def _without_empty_values(self, properties: Dict[str, Any]) -> Dict[str, Any]:
        """Remove empty values and make nested values JSON safe."""
        cleaned = {}
        for key, value in properties.items():
            if value is None or value == "":
                continue
            if isinstance(value, dict) and "@id" in value:
                cleaned[key] = value
            else:
                cleaned[key] = self._json_safe_value(value)
        return cleaned

    def _add_xcom_additional_property_entities(
        self,
        crate: ROCrate,
        reference_id: str,
        value: Dict[str, Any],
    ) -> List[Dict[str, str]]:
        """Keep extra XCom fields as linked property values."""
        core_keys = {
            "name",
            "description",
            "uri",
            "content_url",
            "contentUrl",
            "url",
            "identifier",
            "service",
            "provider",
            "system",
            "service_name",
            "serviceName",
            "service_type",
            "serviceType",
            "service_url",
            "serviceUrl",
            "checksum",
            "sha256",
            "md5",
            "media_type",
            "mediaType",
        }
        additional_properties = []
        for property_name, property_value in sorted(value.items()):
            if property_name in core_keys or property_value is None or property_value == "":
                continue
            property_id = f"{reference_id}_property_{self._safe_identifier(property_name)}"
            property_entity = ContextEntity(
                crate,
                property_id,
                properties={
                    "@type": "PropertyValue",
                    "name": property_name,
                    "propertyID": property_name,
                    "value": self._json_safe_value(property_value),
                },
            )
            crate.add(property_entity)
            additional_properties.append({"@id": property_id})
        return additional_properties

    def _first_present(self, value: Dict[str, Any], *keys: str) -> Any:
        """Return the first non empty value for these keys."""
        for key in keys:
            candidate = value.get(key)
            if candidate is not None and candidate != "":
                return candidate
        return None
