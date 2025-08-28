
# import all modules, libraries

import airflow
from airflow import DAG
from datetime import datetime,timedelta
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


#variable section
PROJECT_ID = 'centered-sol-469812-v8'

DATASET_NAME_1 = 'raw_ds'
DATASET_NAME_2 = 'insight_ds'

TABLE_NAME_1 = 'emp_raw'
TABLE_NAME_2 = 'dept_raw'
TABLE_NAME_3 = 'empDep_in'

LOCATION = "US"

INSERT_ROWS_QUERY = f"""
CREATE TABLE `{PROJECT_ID}.{DATASET_NAME_2}.{TABLE_NAME_3}` AS
SELECT 
    e.EmployeeID,
    concat(e.FirstName, ' ', e.LastName) AS FullName,
    e.Email,
    e.Salary,
    e.JoinDate,
    d.Dept_id,
    d.Dept_name,
    CAST(e.Salary AS INTEGER) * 0.01 AS Tax
FROM
    `{PROJECT_ID}.{DATASET_NAME_1}.{TABLE_NAME_1}` e
LEFT JOIN
    `{PROJECT_ID}.{DATASET_NAME_1}.{TABLE_NAME_2}` d
ON e.DepartmentID = d.Dept_id
WHERE e.EmployeeID is not null
"""

ARGS = {
    "owner" : "Onkar Shitole",
    "start_date" : datetime(2025,8,26),
    "retries" : 1,
    "retry_delay": timedelta(minutes=1)
}


# define the DAG
with DAG(
    "level_1_dag",
    schedule_interval = "7 18 * * *",
    default_args = ARGS
) as dag:

# define tasks
    task_1 = GCSToBigQueryOperator(
        task_id="emp_task",
        bucket="my-bucket-88",
        source_objects=["employee.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_NAME_1}.{TABLE_NAME_1}",
        schema_fields=[
            {"name": "EmployeeID", "type": "INT64", "mode": "NULLABLE"},
            {"name": "FirstName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "LastName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "DepartmentID", "type": "INT64", "mode": "NULLABLE"},
            {"name": "Salary", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "JoinDate", "type": "STRING", "mode": "NULLABLE"},
    ],
    write_disposition="WRITE_TRUNCATE",
    )



    task_2 = GCSToBigQueryOperator(
        task_id="dept_task",
        bucket="my-bucket-88",
        source_objects=["departments.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_NAME_1}.{TABLE_NAME_2}",
        schema_fields=[
            {"name": "Dept_id", "type": "INT64", "mode": "NULLABLE"},
            {"name": "Dept_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Manager_id", "type": "INT64", "mode": "NULLABLE"},
            {"name": "Total_emp", "type": "INT64", "mode": "NULLABLE"},
    ],
    write_disposition="WRITE_TRUNCATE",
    )

    task_3 = BigQueryInsertJobOperator(
    task_id="insert_query_job",
    configuration={
        "query": {
            "query": INSERT_ROWS_QUERY,
            "useLegacySql": False,
            "priority": "BATCH",
        }
    },
    location=LOCATION,
    )


# define dependency
(task_1,task_2) >> task_3
