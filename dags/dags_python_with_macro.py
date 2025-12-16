from airflow.sdk import DAG, task
import datetime
import pendulum
from airflow.providers.standard.operators.python import PythonOperator  # 함수를 실행시키는 오퍼레이터

with DAG(
    dag_id="dags_python_with_macro",
    schedule="10 0 * * *",  # 분 시 일 월 요일
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"), 
    catchup=False
) as dag:
    
    @task(task_id = 'task_using_macros',
          templates_dict = { 'start_date': '{{data_interval_end.in_timezone("Asia/Seoul").subtract(months=1).replace(day=1) | ds}}'  # 전월 1일
                           , 'end_date'  : '{{data_interval_end.in_timezone("Asia/Seoul").subtract(months=1).end_of("month") | ds}}' # 전월 말일
          }
    )
    def get_datetime_macro(**kwargs):
        templates_dict = kwargs.get('templates_dict') or {}
        if templates_dict :
            start_date = templates_dict.get('start_date') or 'start_date 없음'
            end_date   = templates_dict.get('end_date') or 'end_date 없음'
            print(f'start_date : {start_date}')
            print(f'end_date : {end_date}')

    @task(task_id = 'task_direct_calc')
    def get_datetime_calc(**kwargs):
        '''
        스케쥴러는 주기적으로 파일들을 파싱하는데 dag 전에 import 하는 부분, as dag: 이후에 task 적는 부분들을 주기적으로 검사함(파싱)
        그래서 그 쪽에 뭐가 많으면 스케쥴러 부하가 일어남 그래서 웬만하면 task 안, 함수 안에서 import 할 수 있는건 안에 넣는게 좋음
        '''
        from dateutil.relativedelta import relativedelta

        data_interval_end = kwargs.get('data_interval_end')

        prev_month_day_first = data_interval_end.in_timezone("Asia/Seoul") + relativedelta(months = -1, day = 1)
        prev_month_day_last  = data_interval_end.in_timezone("Asia/Seoul").replace(day = 1) + relativedelta(days = -1)
        print(f"prev_month_day_first : {prev_month_day_first.strftime('%Y-%m-%d')}")
        print(f"prev_month_day_last : {prev_month_day_last.strftime('%Y-%m-%d')}")


    get_datetime_macro() >> get_datetime_calc()