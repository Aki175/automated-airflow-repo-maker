# Example Data Files

`sample_measurements.csv` is a small input file used by `edge_case_parallel_data_dag`.

To test manual file inclusion in the reproducibility package, capture the DAG with:

```bash
.venv/bin/airflow-rocrate capture \
  --dag-id edge_case_parallel_data_dag \
  --include-data data/sample_measurements.csv
```

You can also add your own PDF or other manual artifact, for example:

```bash
.venv/bin/airflow-rocrate capture \
  --dag-id edge_case_parallel_data_dag \
  --include-data data/sample_measurements.csv \
  --include-data data/manual_protocol.pdf
```

The DAG records the CSV path, size, SHA-256 checksum, and summary statistics in XCom. The `--include-data` option physically copies the selected file into the package `data/` folder.
