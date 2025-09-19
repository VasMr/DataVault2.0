from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import os

default_args = {
    "owner": "airflow",
    "tags": ["DV_2.0"]
}

sql_file = os.path.join("sql", "ddl_funk.sql")

with DAG(
        dag_id="ddl_creator",
        start_date=datetime(2025, 1, 1),
        schedule_interval=None,
        catchup=False,
        default_args=default_args
) as dag:
    ddl_creator = SQLExecuteQueryOperator(
        task_id="create_ddl",
        conn_id="postgres_conn",
        sql=sql_file
    )

ddl_creator

