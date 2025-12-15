from airflow.sdk import DAG
import datetime
import pendulum
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_select_fruit",
    schedule="10 0 * * 6#1",  # 첫번째주 토요일 0시 10분에 돌아라
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),  # dag이 언제부터 돌건지 정해줌 utc기준으로 돌면 안됨
    catchup=False,                                              # 일반적으로 false로 둠
) as dag:
    t1_orange = BashOperator(
        task_id="t1_orange",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh ORANGE"  #워커 컨데이터가 shell 프로그램이 위치를 알 수 있도록 명시
    )

    t2_avocado = BashOperator(
        task_id="t2_avocado",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh AVOCADO"
    )

    t1_orange >> t2_avocado