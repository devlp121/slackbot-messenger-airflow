"""Get Chess Players Leaderboard, Transform and Send To Slack Channel"""
from __future__ import annotations

import json
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.slack.notifications.slack import SlackNotifier
from airflow.providers.slack.operators.slack import SlackAPIOperator, SlackAPIPostOperator
from airflow.decorators import dag, task

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "slackbot_messenger"


dag = DAG(
    DAG_ID,
    default_args={"retries": 1},
    tags=["simple"],
    start_date=datetime(2023, 7, 9),
    catchup=False,
)

dag.doc_md = __doc__


task_get_chess_leaderboard = SimpleHttpOperator(
    task_id="get_chess_leaderboard",
    method="GET",
    http_conn_id="chess_leaderboard",
    headers={"Content-Type": "application/json"},
    log_response= True,
    dag=dag,
)

def transform_query(**kwargs):
    list_names = ["You"]
    ti = kwargs["ti"]
    response = ti.xcom_pull(task_ids='get_chess_leaderboard')
    json_file = json.loads(response)
    json_file = json_file["daily"]
    for element in json_file:
        list_names.append(element['username'])

    return list_names

transform_task = PythonOperator(
   task_id="transform_query",
   provide_context=True,
   python_callable=transform_query,
   dag=dag,
)

slack_message_task = SlackAPIPostOperator(
    task_id="post_message_slack",
    slack_conn_id="slack_conn_id",
    dag=dag,
    text="{{task_instance.xcom_pull(task_ids='transform_query')}}",
    channel="#random",
)

task_get_chess_leaderboard >> transform_task >> slack_message_task
