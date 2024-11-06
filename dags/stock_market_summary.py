import pendulum

from airflow import DAG

from common.tasks import get_stock_list
from common.tasks import all_stock_price
from common.tasks import all_exchange_rate
from common.tasks import all_aitimes_news
from common.tasks import all_hankyoung_news
from common.tasks import send_slack_message

from airflow.operators.python import PythonOperator
from datetime import datetime

KST = pendulum.timezone("Asia/Seoul")
default_args = {
    "start_date": datetime(2021, 1, 1, tzinfo=KST),
}

with DAG(
    dag_id='stock-market-summary',
    schedule_interval="@daily",
    default_args=default_args,
    tags=["project", "stock-market"],
    catchup=False,
) as dag:
    stock_list = PythonOperator(
        task_id='stock_list',
        python_callable=get_stock_list
    )

    stock_price = PythonOperator(
        task_id="stock_price",
        python_callable=all_stock_price,
        provide_context=True
    )

    exchange_rate = PythonOperator(
        task_id="exchange_rate",
        python_callable=all_exchange_rate,
        provide_context=True
    )

    aitimes_news = PythonOperator(
        task_id="aitimes_news",
        python_callable=all_aitimes_news,
        provide_context=True
    )

    hankyoung_news = PythonOperator(
        task_id="hankyoung_news",
        python_callable=all_hankyoung_news,
        provide_context=True
    )

    send_slack_message = PythonOperator(
        task_id="send_slack_message",
        python_callable=send_slack_message,
        provide_context=True
    )

    stock_list >> stock_price >> [exchange_rate, aitimes_news, hankyoung_news] >> send_slack_message
    # stock_list >> stock_price >> exchange_rate >> aitimes_news >> hankyoung_news >> send_slack_message
