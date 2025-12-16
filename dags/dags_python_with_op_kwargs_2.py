from airflow.sdk import DAG, task
import datetime
import pendulum
from airflow.providers.standard.operators.python import PythonOperator  # 함수를 실행시키는 오퍼레이터
from common.common_func import regist2

 
with DAG(
    dag_id="dags_python_with_op_kwargs_2", 
    schedule="30 6 * * *",  # 분 시 일 월 요일
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:

    regist_t1 = PythonOperator (
        task_id = 'regist_t1',
        python_callable = regist2,
        op_kwargs= {'phone':'010'},
        op_args = ['wjh', 'w', 'kr']
    )
    
    