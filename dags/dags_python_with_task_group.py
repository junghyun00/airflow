from airflow.sdk import DAG, task, task_group
import datetime
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils import TaskGroup



with DAG(
    dag_id="dags_python_with_task_group",
    schedule="10 0 * * *",  # 분 시 일 월 요일
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"), 
    catchup=False
) as dag:
    def inner_func(**kwagrs) :
        msg = kwagrs.get('msg') or ''
        print(msg)

    '''
    1. 데커레이터 이용해서 그룹 생성
    '''
    @task_group(group_id = 'first_group')
    def group_1():
        ''' task_group 데커레이터를 이용한 첫번째 그룹입니다. '''  #docstring : 이렇게 그룹 내에 주석처리 해두면 그룹에 대한 설명이 airflow에 저장돼서 ui에 보여짐

        @task(task_id = 'inner_func1')
        def inner_func1(**kwagrs):
            print('첫번째 taskgroup내 첫번째 task입니다.')

        @task(task_id = 'inner_func2')
        def inner_func2(**kwagrs):
            print('첫번째 taskgroup내 두번째 task입니다.')

        inner_func1() >> inner_func2()


    '''
    2. 클래스를 이용해서 그룹 생성
    '''
    with TaskGroup(group_id = 'second_group', tooltip = '두번째 그룹입니다.') as group_2:   # tooltip = docstring

        @task(task_id = 'inner_func1')
        def inner_func1(**kwagrs):
            print('두번째 taskgroup내 첫번째 task입니다.')

        @task(task_id = 'inner_func2')
        def inner_func2(**kwagrs):
            print('두번째 taskgroup내 두번째 task입니다.')

        inner_func1() >> inner_func2()

    
    group_1() >> group_2