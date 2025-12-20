from airflow.sdk import DAG, task
import datetime
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.exceptions import AirflowException

with DAG(
    dag_id="dags_python_with_trigger_rlue_eg1",
    schedule="10 0 * * *",  # 분 시 일 월 요일
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"), 
    catchup=False
) as dag:
    bash_upstream_1 = BashOperator(
        task_id = 'bash_upstream_1',
        bash_command='echo upstream1'
    )

    @task(task_id = 'python_upstream_1')
    def python_upstream_1():
        raise AirflowException('downstream_1 Excption')   # 실패(에러)하도록 지정함

    @task(task_id = 'python_upstream_2')
    def python_upstream_2():
        print('정상 처리')

    @task(task_id = 'python_downstream_1', trigger_rule = 'all_done')  # 상위Task가 모두 수행되면 실행 (실패도 수행된 것에 포함)
    def python_downstream_1():
        print('정상 처리')

    [bash_upstream_1, python_upstream_1(), python_upstream_2()] >> python_downstream_1()