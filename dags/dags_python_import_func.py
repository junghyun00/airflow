from airflow.sdk import DAG
import datetime
import pendulum
from airflow.providers.standard.operators.python import PythonOperator  # 함수를 실행시키는 오퍼레이터
from common.common_func import get_sftp
# .env 파일을 통해 pythonpath를 plugins까지로 설정 해놨기 때문에 common.common_func 불러올때는 plugins 지정 안해도 됨
# .env 파일은 wsl에 안 올라가도 돼서 gitignore에 적음

 
with DAG(
    dag_id="dags_python_import_func", 
    schedule="30 6 * * *",  # 분 시 일 월 요일
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:

    task_get_sftp = PythonOperator (
        task_id = 'task_get_sftp',
        python_callable = get_sftp # 실행시킬 함수 지정
    )
    
    