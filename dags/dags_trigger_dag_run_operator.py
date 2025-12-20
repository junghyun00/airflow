from airflow.sdk import DAG, task
import datetime
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id="dags_trigger_dag_run_operator",
    schedule="10 0 * * *",  # 분 시 일 월 요일
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"), 
    catchup=False
) as dag:

    start_task = BashOperator(
        task_id = 'start_task',
        bash_command = 'echo "start!"'
    )

    trigger_dag_task = TriggerDagRunOperator(
        task_id = 'trigger_dag_task',
        trigger_dag_id = 'dags_python_operator',       # 실행시키고 싶은 dag
        trigger_run_id = None,                         # run_id를 지정, 지금은 지정 안 함
        conf={"triggered_at": "{{ data_interval_start }}"},    # 실행 시간 지정
        reset_dag_run = True,                          # trigger_run_id을 지정했을때 이미 해당 run_id가 있더라도 리셋하고 실행시킬 것인가
        wait_for_completion = False,                   # trigger 된 dag이 성공이 되던말던 해당 task는 성공으로 찍힘 그리고 다음 task가 있다면 넘어감 # True여야지 선후관계가 생겨서 뒤에 task가 실행이 안됨
        poke_interval = 60,                            # 트리거 된 dag이 실행이 끝났는지 안 끝났는지 모니터링 주기, 지금은 60초마다 확인 함
        allowed_states = ['success'],                  # 해당 task를 success로 끝내기 위해선 트리거 된 dag이 어떤 상태로 끝나야하는지 정해주는 것, 트리거 된 dag이 fail이어도 해당 task를 success로 찍고 싶으면 리스트에 fail 추가 하면됨
        failed_states=None                             # 해당 task가 fail로 찍히기 위해서 트리거 dag이 어떤 상태로 끝나야하는가 ex)해당 값 fail로 주면 트리거 dag fail이면 task로 fail이 됨
    )


    start_task >> trigger_dag_task