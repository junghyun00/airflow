from airflow.sdk import DAG, task
import datetime
import pendulum
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id="dags_branch_python_operator",
    schedule="10 0 * * *",  # 분 시 일 월 요일
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"), 
    catchup=False
) as dag:
    '''
    A가 선택되면 task_a 돌고
    B,C가 선택되면 task_b, task_c 돌고
    '''
    def select_random():
        import random

        item_list = ['A', 'B', 'C']
        selected_item = random.choice(item_list)
        if selected_item == 'A' :
            return 'task_a'
        elif selected_item in ['B', 'C'] :
            return ['task_b', 'task_c']
        
    python_branch_task = BranchPythonOperator(
        task_id = 'python_branch_task',
        python_callable = select_random
    )



    '''
    여기서 어느 task들이 돌았는지 print로 찍어줌
    '''
    def common_fucn(**kwagrs):
        print(kwagrs.get('selected'))

    task_a = PythonOperator(
        task_id = 'task_a',
        python_callable=common_fucn,
        op_kwargs={'selected' : 'A'}
    )

    task_b = PythonOperator(
        task_id = 'task_b',
        python_callable=common_fucn,
        op_kwargs={'selected' : 'B'}
    )

    task_c = PythonOperator(
        task_id = 'task_c',
        python_callable=common_fucn,
        op_kwargs={'selected' : 'C'}
    )



    python_branch_task >> [task_a, task_b, task_c]