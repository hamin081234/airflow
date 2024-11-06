from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

default_args = {
    'start_date': datetime(2019, 11, 1),
}


def task_1():
    return "task 1 done!"


def task_2(**kwargs):
    message = kwargs['ti'].xcom_pull(task_ids='task_1')
    slack_message = SlackAPIPostOperator(
        task_id='slack_message',
        channel='#일반',
        text=message,
    )
    slack_message.execute(context=kwargs)


with DAG(
    dag_id='slack-message-tutorial',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
) as dag:
    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=task_1
    )

    send_message_1 = SlackAPIPostOperator(
        task_id='send_message_to_slack',
        channel='#일반',
        text='Successfully sent notification to Slack with SlackAPIPostOperator',
    )

    task_2 = PythonOperator(
        task_id='task_2',
        python_callable=task_2
    )

    task_1 >> send_message_1 >> task_2
