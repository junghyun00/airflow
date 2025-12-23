from airflow.sdk import DAG
from datetime import timedelta 
import pendulum
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import State

with DAG(
    dag_id="dags_external_task_sensor", 
    schedule='*/1 * * * *',
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    '''
    ExternalTaskSensor는 dag의 task를 보는 게 아니라 “특정 시각에 실행된 task 인스턴스”를 본다
    그러므로 내 execution_date와 동일한 execution_date를 가진 task를 명시를 해줘야하는 것

    Airflow에서 task는 (dag_id, task_id, 실행시간)이 합쳐진 개념임
    그래서 ExternalTaskSensor는 외부의 task를 sensor하는거니까 execution_delta까지 무조건 디폴트로 써줘야함 key값임
    '''
    
    external_task_sensor_a = ExternalTaskSensor(
        task_id= 'external_task_sensor_a',
        external_dag_id = 'dags_branch_python_operator',
        external_task_id = 'task_a',

        # allowed_states : 모니터링 하는 task_a가 skipped 상태일때만!! 해당 task는 success가 됨 / task_a가 success가 떠도 skipped일때만 success인거니까 계속 porking함
        allowed_states = [State.SKIPPED], 
        execution_delta = timedelta(minutes=1),
        poke_interval=10   #10초
    )

    external_task_sensor_b = ExternalTaskSensor(
        task_id= 'external_task_sensor_b',
        external_dag_id = 'dags_branch_python_operator',
        external_task_id = 'task_b',

        # allowed_states = [State.SUCCESS]는 디폴트 값이라 명시 안 해줘도 선언되어 있는거임 / 그래서 task_b가 success가 뜨면 해당 sensor도 success임
        failed_states = [State.SKIPPED],   # failed_states : 모니터링 하는 task_b가 skipped 상태가 되면 해당 task는 fail됨
        execution_delta = timedelta(minutes=1),
        poke_interval=10   #10초
    )

    external_task_sensor_c = ExternalTaskSensor(
        task_id= 'external_task_sensor_c',
        external_dag_id = 'dags_branch_python_operator',
        external_task_id = 'task_c',

        # task_c가 skipped일 경우 allowed_states에 SKIPPED가 없고 failed_states에도 SKIPPED가 없기 때문에 → 조건 미충족 → 계속 poking
        allowed_states = [State.SUCCESS],  # allowed_states : 모니터링 하는 task_c가 SUCCESS 상태가 되면 해당 task는 success가 됨
        execution_delta = timedelta(minutes=1),
        poke_interval=10   #10초
    )