import logging

import duckdb
import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


# Конфигурация DAG
OWNER = "i.korsakov"
DAG_ID = "dm_dag_without_sensors"

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
    Печатает контекст DAG.

    @param context: Контекст DAG.
    @return: Ничего не возвращает.
    """
    query = f"""
        INSTALL postgres;
        LOAD postgres;
        ATTACH 'dbname=postgres user=postgres host=dwh password=postgres' AS db (TYPE postgres);

        CREATE SCHEMA IF NOT EXISTS db.dm;
        CREATE SCHEMA IF NOT EXISTS db.stg;

        CREATE TABLE IF NOT EXISTS db.dm.dm_count_registered_users
        (
            created_at TIMESTAMP PRIMARY KEY,
            count_registered_users BIGINT
        );

        DROP TABLE IF EXISTS db.stg.stg_count_registered_users;

        CREATE TABLE db.stg.stg_count_registered_users AS
        SELECT
            DATE_TRUNC('day', created_at) AS created_at,
            COUNT(id) AS count_registered_users
        FROM
            db.ods.ods_user
        WHERE
            DATE_TRUNC('day', created_at) = '{context.get("data_interval_start").format("YYYY-MM-DD")}'
        GROUP BY 1;

        DELETE FROM db.dm.dm_count_registered_users
        WHERE created_at IN (SELECT created_at FROM db.stg.stg_count_registered_users);

        INSERT INTO db.dm.dm_count_registered_users
        SELECT
            created_at,
            count_registered_users
        FROM
            db.stg.stg_count_registered_users;

        DROP TABLE db.stg.stg_count_registered_users;
        """

    logging.info("Loading DM layer... ⏳ with query:\n%s", query)

    duckdb.sql(query=query)

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

    start >> load_dm_layer >> end
