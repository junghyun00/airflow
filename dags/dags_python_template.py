from airflow.sdk import DAG, task
import datetime
import pendulum
from airflow.providers.standard.operators.python import PythonOperator  # 함수를 실행시키는 오퍼레이터

with DAG(
    dag_id="dags_python_template",
    schedule="0 0 * * *",  # 분 시 일 월 요일
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"), 
    catchup=False
) as dag:
    
    def python_func1(start_date, end_date, **k):
        print(start_date)
        print(end_date)

    python_t1 = PythonOperator (
        task_id = 'python_t1',
        python_callable=python_func1,
        op_kwargs={'start_date': '{{data_interval_start | ds}}', 'end_date': '{{data_interval_end | ds}}'}
    )

    @task(task_id = 'python_t2')
    def python_func2(**ka):
        print(ka)
        print('ds : ' + ka.get('ds'))
        print('ts : ' + ka.get('ts'))
        print('data_interval_start : ' + str(ka.get('data_interval_start')))
        print('data_interval_end : ' + str(ka.get('data_interval_end')))
        print('task_instance : ' + str(ka.get('ti')))

    python_t1 >> python_func2()