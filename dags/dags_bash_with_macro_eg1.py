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
        task_id = 'bash_task_1',
        # env = {'start_date' : '{{data_interval_start.in_timezone("Asia/Seoul") | ds}}',
        #        'end_date' : '{{(data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds}}'
        # },
        env = {'start_date' : '{{data_interval_end.in_timezone("Asia/Seoul").subtract(months=1).end_of("month") | ds}}',
               'end_date' : '{{macros.ds_add(data_interval_end.in_timezone("Asia/Seoul") | ds, -1)}}'
        },
        bash_command = 'echo "start_date : $start_date" && echo "end_date : $end_date"'
    )



    # 연습
    bash_task_2 = BashOperator(
        task_id = 'bash_task_2',
        env = {'a' : '{{data_interval_end.in_timezone("Asia/Seoul").subtract(months=1) | ds}}',
               'b' : '{{data_interval_end.in_timezone("Asia/Seoul").subtract(months=1).end_of("month") | ds}}',
               'c' : '{{macros.ds_add(data_interval_end.in_timezone("Asia/Seoul") | ds, -1)}}'
               'd' : '{{data_interval_end.in_timezone("Asia/Seoul").start_of("month") | ds}}',
               'e' : '{{data_interval_end.in_timezone("Asia/Seoul").subtract(months=1).start_of("month") | ds}}',
               'f' : '{{data_interval_end.in_timezone("Asia/Seoul").subtract(months=6).start_of("month") | ds}}'
        },
        bash_command = 'echo "1달 전 : $a" && echo "전월 마지막 일 : $b" && echo "하루 전 : $c" && echo "이번 달 1일: $d" && echo "전월 1일 : $e" && echo "6개월 전 1일 : $f"'
    )


    bash_task_1 >> bash_task_2