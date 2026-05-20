"""
Tools for packaging Airflow DAG runs with RO-Crate metadata.
"""

__version__ = "0.1.0"
__author__ = "Akbar"

__all__ = [
    "AirflowMetadataCapture",
    "RocrateCrateGenerator",
    "ReproducibilityPackager",
]


def __getattr__(name):
    if name == "AirflowMetadataCapture":
        from airflow_rocrate.metadata import AirflowMetadataCapture

        return AirflowMetadataCapture
    if name == "RocrateCrateGenerator":
        from airflow_rocrate.rocrate_generator import RocrateCrateGenerator

        return RocrateCrateGenerator
    if name == "ReproducibilityPackager":
        from airflow_rocrate.packager import ReproducibilityPackager

        return ReproducibilityPackager
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
