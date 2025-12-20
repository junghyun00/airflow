from airflow.sdk import DAG, task, task_group, TaskGroup
import datetime
import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label

with DAG(
    dag_id="dags_empty_with_edge_label",
    schedule="10 0 * * *",  # 분 시 일 월 요일
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"), 
    catchup=False
) as dag:
    # 1
    empty_1 = EmptyOperator(
        task_id = 'empty_1'
    )

    empty_2 = EmptyOperator(
        task_id = 'empty_2'
    )

    empty_1 >> Label('1과 2사이') >> empty_2


    # 2
    empty_3 = EmptyOperator(
        task_id = 'empty_3'
    )
    empty_4 = EmptyOperator(
        task_id = 'empty_4'
    )
    empty_5 = EmptyOperator(
        task_id = 'empty_5'
    )
    empty_6 = EmptyOperator(
        task_id = 'empty_6'
    )


    empty_2 >> Label('START') >> [empty_3, empty_4, empty_5] >> Label('END') >> empty_6