from airflow.sdk import DAG
import datetime
import pendulum
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_operator",
    schedule="0 0 * * *",  # 분 시 일 월 요일
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),  # dag이 언제부터 돌건지 정해줌 utc기준으로 돌면 안됨
    catchup=False,                                              # 일반적으로 false로 둠
    # dagrun_timeout=datetime.timedelta(minutes=60),   # dag이 60분 이상으로 돌면 타임아웃 시키는 것
    # tags=["example", "example2"],                      # dag마다 태그를 달 수 있음
    # params={"example_key": "example_value"},           # dag안에 task들에 공통적으로 넘겨줄 파라미터들을 명시
) as dag:
    bash_t1 = BashOperator(    # task는 오퍼레이터들을 통해서 만들어짐
        task_id="bash_t1",     # 웹에서 그래프의 이름 (객체=task의 이름과 같이 쓰는게 일반적)
        bash_command="echo whoami",   # echo = print 명령어랑 똑같음
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME",
    )

    bash_t1 >> bash_t2