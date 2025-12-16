from airflow.sdk import DAG
import datetime
import pendulum
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_macro_eg1",
    schedule="10 0 L * *",  # 매월 말일
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    # start_date : 전월 말일, end_date : 1일 전
    bash_task_1 = BashOperator(
        task_id = 'bash_task_1'
        env = {'start_date' : '{{data_interval_start.in_timezone("Asia/Seoul") | ds}}',
               'end_date' : '{{(data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds}}'
        },
        bash_command = 'echo "start_date : $start_date" && echo "end_date : $end_date"'
    )