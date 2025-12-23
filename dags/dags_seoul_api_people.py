from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
#from airflow import DAG

with DAG(
    dag_id='dags_seoul_api_people',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2025,4,1, tz='Asia/Seoul'), 
    catchup=False
) as dag:
    '''행정동 단위 서울 생활인구(내국인).'''
    tb_seoul_people = SeoulApiToCsvOperator(
        task_id='tb_seoul_people',
        dataset_nm='SPOP_LOCAL_RESD_DONG',   # 이 데이터셋 이름은 꼭 api에 있는걸로 해야함 오퍼레이터를 그렇게 만들었음
        path='/opt/airflow/files/SPOP_LOCAL_RESD_DONG/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='SPOP_LOCAL_RESD_DONG.csv',
        base_dt = '20251219'
    )


    tb_seoul_people 