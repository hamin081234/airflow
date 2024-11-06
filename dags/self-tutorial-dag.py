from datetime import datetime, timedelta
import json
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import SQLExecuteQueryOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator

default_args = {
    "start_date": datetime(2021, 1, 1),
}


def start_task(**kwargs):
    return "About to fall a sleep"


def echo_input(**kwargs):
    message = kwargs['ti'].xcom_pull(task_ids='start_task')
    print('#'*50+message)


with DAG(
    dag_id='self-tutorial-dag',
    schedule_interval="@daily",
    default_args=default_args,
    tags=["self-tutorial"],
    catchup=False,
) as dag:
    # 네이버 API로 지역 식당 검색
    # 지역 식당명, 주소, 카테고리, 설명, 링크를 저장할 테이블 구성
    creating_table = SQLExecuteQueryOperator(
        task_id='creating_table',
        conn_id="db_postgres",  # 웹 UI에서 connection을 등록해줘야 함
        # naver_search_result 라는 테이블이 없는 경우에만 만들도록 IF NOT EXISTS 조건을 넣기
        sql="""
            create table if not exists naver_search_result(
                title TEXT,
                address TEXT,
                category TEXT,
                description TEXT,
                link TEXT
            )
        """
    )

    start_task = PythonOperator(
        task_id="start_task",
        python_callable=start_task
    )

    echo_input = PythonOperator(
        task_id="echo_input",
        python_callable=echo_input,
        provide_context=True
    )

    creating_table >> start_task >> echo_input
