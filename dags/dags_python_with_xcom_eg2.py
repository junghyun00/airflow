from airflow.sdk import DAG, task
import datetime
import pendulum


with DAG(
    dag_id="dags_python_with_xcom_eg2",
    schedule="10 0 * * *",  # 분 시 일 월 요일
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"), 
    catchup=False
) as dag:
    
    @task(task_id = 'python_xcom_push_by_return')
    def xcom_push_result(**k):
        return 'Success'


    @task(task_id = 'python_xcom_pull_1')
    def xcom_pull_1(**k):
        ti = k.get('ti')
        value1 = ti.xcom_pull(task_ids = 'python_xcom_push_by_return')
        print('xcom_pull 메서드로 직접 찾은 리턴 값 : ' + value1)


    @task(task_id = 'python_xcom_pull_2')
    def xcom_pull_2(status, **k):
        print('함수 입력값으로 받은 값은 : ' + status)


    python_xcom_push_by_return = xcom_push_result()
    xcom_pull_2(python_xcom_push_by_return)
    python_xcom_push_by_return >> xcom_pull_1() 