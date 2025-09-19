from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

defaul_args = {
    "owner": "airflow",
    "tag": ["DV_2.0"]
}


def load_raw_emails():
    import requests
    import json

    response = requests.get('https://jsonplaceholder.typicode.com/posts/')

    if response.status_code >= 400:
        raise Exception("Не удалось получить данные")

    raw_data = response.json()
    raw_data = [(json.dumps(r),) for r in raw_data]

    hook = PostgresHook(postgres_conn_id="postgres_conn")
    hook.insert_rows(
        table="stg.raw_json",
        rows=raw_data,
        target_fields=["json"],
        commit_every=10
    )

with DAG(
        dag_id="load_stg",
        start_date=datetime(2025, 1, 1, 0, 0, 0),
        schedule_interval=None,
        catchup=False,
        default_args=defaul_args
) as dag:
    load_data = PythonOperator(
        task_id="load_data_from_api",
        python_callable=load_raw_emails,
    )
    trigger_dds_load = TriggerDagRunOperator(
        task_id="trigger_next_layer_load",
        trigger_dag_id="load_dds",
        wait_for_completion=False
    )

load_data >> trigger_dds_load

