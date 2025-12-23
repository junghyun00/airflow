from airflow.sdk import DAG
import datetime
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor


with DAG(
    dag_id="dags_file_sensor", 
    schedule="30 6 * * *",  # 분 시 일 월 요일
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    bikeStationMaster_sensor = FileSensor(
        task_id = 'bikeStationMaster_sensor',
        fs_conn_id = 'conn_file_opt_airflow_files',
        filepath = 'bikeStationMaster/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/bikeStationMaster.csv',
        recursive = False,    # 해당 파일 경로 밑에 있는 다른 폴더, 파일까지 다 가져오는가
        poke_interval = 60,
        timeout = 60*60*24,  # 1dlf
        mode = 'reschedule'
    )