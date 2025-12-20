from airflow.sdk import DAG, task
import datetime
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.exceptions import AirflowException

with DAG(
    dag_id="dags_python_with_trigger_rlue_eg2",
    schedule="10 0 * * *",  # 분 시 일 월 요일
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"), 
    catchup=False
) as dag:
    @task.branch(task_id = 'branching')
    def select_random():
        import random

        item_list = ['A', 'B', 'C']
        selected_item = random.choice(item_list)
        if selected_item == 'A' :
            return 'task_a'
        elif selected_item == 'B' :
            return 'task_b'
        elif selected_item == 'C' :
            return 'task_c'

    task_a = BashOperator(
        task_id = 'task_a',
        bash_command='echo upstream1'
    )

    @task(task_id = 'task_b')
    def task_b():
        print('정상 처리')

    @task(task_id = 'task_c')
    def task_c():
        print('정상 처리')

    @task(task_id = 'task_d', trigger_rule = 'none_skipped') # Skip된 상위 Task가 없으면 실행 (상위Task가 성공, 실패여도 무방)
    def task_d():
        print('정상 처리')


    select_random() >> [task_a, task_b(), task_c()] >> task_d()