from airflow.sdk import DAG, task
import datetime
import pendulum


with DAG(
    dag_id="dags_python_show_templates",
    schedule="30 6 * * *",  # 분 시 일 월 요일
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"),
    catchup=True
) as dag:
    
    @task(task_id="python_task")
    def show_templates(**k):
        from pprint import pprint
        pprint(k)

    show_templates()