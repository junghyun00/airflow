import pendulum
from airflow.hooks.base import BaseHook

# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG
from airflow.providers.standard.sensors.python import PythonSensor

# Airflow 2.10.5 이하 버전에서 실습시 airflow.sensors.python 에서 import 하세요.
#from airflow import DAG
#from airflow.sensors.python import PythonSensor



with DAG(
    dag_id='dags_python_sensor',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    schedule='10 1 * * *',
    catchup=False
) as dag:
    def check_api_update(http_conn_id, endpoint, base_dt_col, **kwargs):
        import requests
        import json
        from dateutil import relativedelta
        connection = BaseHook.get_connection(http_conn_id)
        url = f'http://{connection.host}:{connection.port}/{endpoint}/1/100'
        response = requests.get(url)
        
        contents = json.loads(response.text)
        key_nm = list(contents.keys())[0]
        row_data = contents.get(key_nm).get('row')   # row_data 이 변수에 데이터가 딕셔너리 형태로 들어가 있음
        last_dt = row_data[0].get(base_dt_col)       # 첫번째 행의 base_dt_col 키에 대한 value 값을 받아와라
        last_date = last_dt[:10]                     # last_dt가 2023.05.31.00 이렇게 string 형태여서 2023.05.31 형태로 잘라라
        last_date = last_date.replace('.', '-').replace('/', '-')   # 2023-05-31 형태로 바꿔라 
        try:
            pendulum.from_format(last_date,'YYYY-MM-DD')   # 데이터 형식 맞으면 통과, 안 맞으면 except문으로 들어감
        except:
            from airflow.exceptions import AirflowException
            AirflowException(f'{base_dt_col} 컬럼은 YYYY.MM.DD 또는 YYYY/MM/DD 형태가 아닙니다.')

        today_ymd = kwargs.get('data_interval_end').in_timezone('Asia/Seoul').strftime('%Y-%m-%d')
        if last_date >= today_ymd:   # 데이터 존재
            print(f'생성 확인(배치 날짜: {today_ymd} / API Last 날짜: {last_date})')
            return True
        else:  # 데이터 미존재
            print(f'Update 미완료 (배치 날짜: {today_ymd} / API Last 날짜:{last_date})')
            return False   # false면 계속 sensor가 돈다

    sensor_task = PythonSensor(
        task_id='sensor_task',
        python_callable=check_api_update,
        op_kwargs={'http_conn_id':'openapi.seoul.go.kr',
                   'endpoint':'{{var.value.apikey_openapi_seoul_go_kr}}/json/TbCorona19CountStatus',   # 서울시 코로나19 확진자(전수감시) 발생동향 (2023.08.31.이전)
                   'base_dt_col':'S_DT'},
        poke_interval=60,   #1분
        timeout = 60*5,
        mode='reschedule'
    )