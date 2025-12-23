import pendulum
from airflow.sdk import DAG
from sensors.seoul_api_date_sensor import SeoulApiDateSensor


with DAG(
    dag_id='dags_custom_sensor',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    schedule='10 1 * * *',
    catchup=False
) as dag:

    api_sensor = SeoulApiDateSensor(
        task_id='api_sensor',
        dataset_nm = 'TbCorona19CountStatus',
        base_dt_col = 'S_DT',
        day_off = 0,
        poke_interval=600,   #1분
        mode='reschedule'
    )
