from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "tags": ["DV_2.0"]
}

with DAG(
        dag_id="load_dds",
        start_date=datetime(2025, 1, 1),
        schedule_interval=None,
        catchup=False,
        default_args=default_args
) as dag:
    load_hub_user = SQLExecuteQueryOperator(
        task_id="hub_user_loader",
        conn_id="postgres_conn",
        sql="""
            SELECT dds.load_hub_user();
            """
    )

    load_hub_email = SQLExecuteQueryOperator(
        task_id="hub_email_loader",
        conn_id="postgres_conn",
        sql="""
            SELECT dds.load_hub_email();
            """
    )

    load_sat_email_meta = SQLExecuteQueryOperator(
        task_id="sat_email_loader",
        conn_id="postgres_conn",
        sql="""
            SELECT dds.load_sat_email_meta();
            """
    )

    load_link_user_email = SQLExecuteQueryOperator(
        task_id="link_user_email_loader",
        conn_id="postgres_conn",
        sql="""
            SELECT dds.load_link_user_email();
            """
    )

load_hub_user >> [load_sat_email_meta, load_link_user_email]
load_hub_email >> [load_sat_email_meta, load_link_user_email]

