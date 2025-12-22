from airflow.sdk import DAG, task
import datetime
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.models import Variable
from airflow.providers.http.operators.http import HttpOperator


with DAG(
    dag_id="dags_simple_http_operator", 
    schedule="30 6 * * *",  # 분 시 일 월 요일
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    '''서울시 공공자전거 대여소 정보'''
    tb_cycle_station_info = HttpOperator(
       task_id =  'tb_cycle_station_info',
       http_conn_id = 'openapi.seoul.go.kr',
       endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/tbCycleStationInfo/1/10/',
       method='GET',
       headers={ 'content-Type':'application/json'
               , 'charset' : 'utf-8'
               , 'Accept' : '*/*'
               }
    )
    # 해당 값이 xcom에 저장됨

    @task(task_id = 'python_2')
    def python_2(**k):
        ti = k.get('ti')
        rslt = ti.xcom_pull(task_ids = 'tb_cycle_station_info')
        import json
        from pprint import pprint

        pprint(json.loads(rslt))


    tb_cycle_station_info >> python_2()