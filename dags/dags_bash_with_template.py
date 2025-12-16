from airflow.sdk import DAG
import datetime
import pendulum
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_template",
    schedule="0 0 * * *",  # 분 시 일 월 요일
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"), 
    catchup=False
) as dag:
    bash_t1 = BashOperator(
        task_id = 'bash_t1',
        bash_command='echo "data_interval_end : {{ data_interval_end }}"'
    )

    bash_t2 = BashOperator(
        task_id = 'bash_t2',
        env = {'start_date':'{{data_interval_start | ds }}',
               'end_date':'{{data_interval_end | ds }}'
              },
        bash_command='echo $start_date && echo $end_date'
    )

    bash_t1 >> bash_t2