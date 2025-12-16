from airflow.sdk import DAG, task
import pendulum


with DAG(
    dag_id="dags_python_task_decorator",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    # [START howto_operator_python]
    @task(task_id="python_task_1")
    def print_context(some_input):
        print(some_input)

    python_task_1 = print_context('task_decorator 실행')
    # [END howto_operator_python]