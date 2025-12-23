from airflow.sdk import DAG
import datetime
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sensors.bash import BashSensor


with DAG(
    dag_id="dags_bash_sensor", 
    schedule="30 6 * * *",  # 분 시 일 월 요일
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    ### sensor 모드 2가지


    '''
    1. poke : 초단위로 sensor를 하고 싶을 때 사용(pool을 계속 잡고 있음)
    '''
    sensor_task_by_poke = BashSensor(
        task_id = 'sensor_task_by_poke',
        env={'file' : '/opt/airflow/files/bikeStationMaster/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/bikeStationMaster.csv'},
        bash_command=f'''echo $file &&
                         if [ -f $file ]; then
                            exit 0
                         else
                            exit 1
                         fi''',
        poke_interval = 30,
        timeout = 60*2,
        mode= 'poke',
        soft_fail=False   # timeout 되면 soft_fail=False여서 fail로 남음
    )
    
    '''
    2. reschedule : 분단위로 sensor를 하고 싶을 때 사용(pool를 잡고 놨다 잡고 놨다를 반복함)
    '''
    sensor_task_by_reschedule = BashSensor(
        task_id = 'sensor_task_by_reschedule',
        env={'file' : '/opt/airflow/files/bikeStationMaster/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/bikeStationMaster.csv'},
        bash_command=f'''echo $file &&
                         if [ -f $file ]; then
                            exit 0
                         else
                            exit 1
                         fi''',
        poke_interval = 60*3,  # 3분
        timeout = 60*5,  # 5분
        mode= 'reschedule',
        soft_fail=True   # timeout 되면 soft_fail=True여서 sikpped로 남음
    )

    bash_task = BashOperator(
        task_id = 'bash_task',
        env={'file' : '/opt/airflow/files/bikeStationMaster/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/bikeStationMaster.csv'},
        bash_command='echo "건수: `cat $file | wc -1`"'
    )

    [sensor_task_by_poke, sensor_task_by_reschedule] >> bash_task