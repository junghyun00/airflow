from airflow.sdk import DAG
import datetime
import pendulum
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_macro_eg2",
    schedule="10 0 * * 6#2 ",  # 매월 2번째 토요일
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    # start_date : 2주전 월요일 , end_date : 2주전 토요일
    bash_task_2 = BashOperator(
        task_id = 'bash_task_2'
        env = {'start_date' : '{{(data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=19)) | ds}}',
               'end_date' : '{{(data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=14)) | ds}}'
        },
        bash_command = 'echo "start_date : $start_date" && echo "end_date : $end_date"'
    )