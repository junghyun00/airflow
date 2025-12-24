from airflow.sdk import DAG, task
import datetime
import pendulum
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_pool",
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"), 
    catchup=False,
    default_args={'pool' : 'pool_small',
    'weight_rule': 'downstream'}   # pool을 설정
) as dag:

    bash_task_1 = BashOperator(
        task_id='bash_task_1',
        bash_command='sleep 30',
        priority_weight = 6
    )

    bash_task_2 = BashOperator(
        task_id='bash_task_2',
        bash_command='sleep 30',
        priority_weight = 5
    )

    bash_task_3 = BashOperator(
        task_id='bash_task_3',
        bash_command='sleep 30',
        priority_weight = 4
    )

    bash_task_4 = BashOperator(
        task_id='bash_task_4',
        bash_command='sleep 30'
        # priority_weight을 안 주면 기본값은 1임
    )

    bash_task_5 = BashOperator(
        task_id='bash_task_5',
        bash_command='sleep 30'
    )

    bash_task_6 = BashOperator(
        task_id='bash_task_6',
        bash_command='sleep 30'
    )

    bash_task_7 = BashOperator(
        task_id='bash_task_7',
        bash_command='sleep 30',
        priority_weight = 7
    )

    bash_task_8 = BashOperator(
        task_id='bash_task_8',
        bash_command='sleep 30',
        priority_weight = 8
    )

    bash_task_9 = BashOperator(
        task_id='bash_task_9',
        bash_command='sleep 30',
        priority_weight = 9
    )


    '''
    task 연결관계를 안 주면 다 동시에 돈다
    그럼 6 5 4 1 1 1 7 8 9 중에서 7 8 9 먼저 돎 pool이  > 그다음 6 5 4 > 그다음 1 1 1
    '''