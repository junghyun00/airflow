from airflow.sdk import DAG, task
import datetime
import pendulum


with DAG(
    dag_id="dags_python_with_xcom_eg1",
    schedule="10 0 * * *",  # 분 시 일 월 요일
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"), 
    catchup=False
) as dag:
    
    @task(task_id = 'python_xcom_push_task1')
    def xcom_push1(**k):
        ti = k.get('ti')
        ti.xcom_push(key = 'result1', value = 'value1')
        ti.xcom_push(key = 'result2', value = [1,2,3])


    @task(task_id = 'python_xcom_push_task2')
    def xcom_push2(**k):
        ti = k.get('ti')
        ti.xcom_push(key = 'result1', value = 'value2')
        ti.xcom_push(key = 'result2', value = [1,2,3,4])


    @task(task_id = 'python_xcom_pull_task')
    def xcom_pull(*k):
        ti = k.get('ti')
        value1 = ti.xcom_pull(key='result1')
        value2 = ti.xcom_pull(key='result2', task_ids = 'python_xcom_push_task1')
        print(value1)
        print(value2)


    xcom_push1() >> xcom_push2() >> xcom_pull()