import logging
import time
import uuid

from random import randint

import duckdb
import pandas as pd
import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from faker import Faker


# Конфигурация DAG
OWNER = "i.korsakov"
DAG_ID = "ods_dag_users_to_dwh_pg"

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


def load_ods_layer(**context) -> None:
    """
    Печатает контекст DAG.

    @param context: Контекст DAG.
    @return: Ничего не возвращает.
    """
    time.sleep(0)

    fake = Faker(locale="ru_RU")

    list_of_dict = []
    for _ in range(randint(a=1, b=100)):
        dict_ = {
            "id": uuid.uuid4(),
            "created_at": context.get("data_interval_start"),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "middle_name": fake.middle_name(),
            "email": fake.email(),
        }

        list_of_dict.append(dict_)

    df = pd.DataFrame(list_of_dict)

    logging.info(f"💿 Размер данных: {df.shape}")
    logging.info(f"🗓️ Загрузка за {context.get('data_interval_start')}")

    query = """
        INSTALL postgres;
        LOAD postgres;
        ATTACH 'dbname=postgres user=postgres host=dwh password=postgres' AS db (TYPE postgres);

        CREATE SCHEMA IF NOT EXISTS db.ods;

        CREATE TABLE IF NOT EXISTS db.ods.ods_user
        (
            id UUID PRIMARY KEY,
            created_at TIMESTAMP,
            first_name VARCHAR,
            last_name VARCHAR,
            middle_name VARCHAR,
            email VARCHAR
        );

        INSERT INTO db.ods.ods_user SELECT * FROM df;
        """

    logging.info("Loading ODS layer... ⏳ with query:\n%s", query)

    duckdb.sql(query=query)

    logging.info("ODS layer loaded success ✅.")


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 10 * * *",
    default_args=args,
    tags=["ods"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    load_ods_layer = PythonOperator(
        task_id="load_ods_layer",
        python_callable=load_ods_layer,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> load_ods_layer >> end
