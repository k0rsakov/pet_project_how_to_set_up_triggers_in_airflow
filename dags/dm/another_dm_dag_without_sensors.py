import logging

import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


# Конфигурация DAG
OWNER = "i.korsakov"
DAG_ID = "another_dm_dag_without_sensors"

LONG_DESCRIPTION = """
# LONG DESCRIPTION

"""

SHORT_DESCRIPTION = "SHORT DESCRIPTION"

# Описание возможных ключей для default_args
# https://github.com/apache/airflow/blob/343d38af380afad2b202838317a47a7b1687f14f/airflow/example_dags/tutorial.py#L39
args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(year=2025, month=1, day=1, tz="UTC"),
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
    "depends_on_past": True,
}


def load_dm_layer(**context) -> None:
    """
    Пустышка.

    @return: Ничего не возвращает.
    """
    logging.info(f"🗓️ Date of start = {context.get('data_interval_start')}")
    logging.info("DM layer loaded success ✅.")


with DAG(
    dag_id=DAG_ID,
    schedule_interval=None,
    default_args=args,
    tags=["dm"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    load_dm_layer = PythonOperator(
        task_id="load_dm_layer",
        python_callable=load_dm_layer,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> sensor_ods_dag_users_to_dwh_pg >> sensor_ods_dag_without_catchup >> load_dm_layer >> end
