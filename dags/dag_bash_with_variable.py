from airflow.sdk import DAG, task
import datetime
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.models import Variable

with DAG(
    dag_id="dag_bash_with_variable",
    schedule="10 0 * * *",  # 분 시 일 월 요일
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"), 
    catchup=False
) as dag:
    '''
    1. BashOperator 밖에서 Variable라는 라이브러리를 import하고 전역변수를 불러온 뒤 저장 (이런 코드는 권고하지 않음)
    '''
    var_value = Variable.get('smaple_key')

    bash_var_1 = BashOperator(
        task_id = 'bash_var_1',
        bash_command = f'echo variable:{var_value}'
    )


    '''
    2. template 변수를 사용해서 전역변수를 직접 명시 함
    '''
    bash_var_2 = BashOperator(
        task_id = 'bash_var_2',
        bash_command = 'echo variable:{{var.value.sample_key}}'
    )