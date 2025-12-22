from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
#from airflow import DAG

with DAG(
    dag_id='dags_seoul_api_bicycle',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2025,4,1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    '''서울시 따릉이대여소 마스터 정보'''
    tb_bicycle_master = SeoulApiToCsvOperator(
        task_id='tb_bicycle_master',
        dataset_nm='bikeStationMaster',
        path='/opt/airflow/files/bikeStationMaster/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='bikeStationMaster.csv'
    )


    tb_bicycle_master