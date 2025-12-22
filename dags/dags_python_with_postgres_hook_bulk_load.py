from airflow.sdk import DAG, task
import datetime
import pendulum
from airflow.providers.standard.operators.python import PythonOperator  # 함수를 실행시키는 오퍼레이터


with DAG(
    dag_id="dags_python_with_postgres_hook_bulk_load", 
    schedule="30 6 * * *",  # 분 시 일 월 요일
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:

    def insert_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from contextlib import closing

        postgres_hook = PostgresHook(postgres_conn_id)
        postgres_hook.bulk_load(tbl_nm, file_nm)

    insrt_postgres_with_hook_bulk_load = PythonOperator(
        task_id = 'insrt_postgres_with_hook_bulk_load',
        python_callable=insert_postgres,
        op_kwargs={ 'postgres_conn_id':'conn-db-postgrs-custom'
                  ,  'tbl_nm':'tb_bulk1'   # 테이블이 디비에 있어야지 에러 안 남
                  , 'file_nm' : {'/opt/airflow/files/bikeStationMaster/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/bikeStationMaster.csv'}
                  # 그리고 데이터는 구분자가 tab으로 구분되어야지 에러 안 남, 그리고 컬럼 헤더 값도 없어야 함
                  }
    )

    '''
    bulk_load의 문제점
    1. load 가능한 구분자는 tab으로 고정되어 있음
    2. 헤더까지 포함해서 업로드 됨
    3. 특수문자로 인해 파싱이 안 될 경우 에러 발생

    >> 이런 문제점들 때문에 custom hook을 만들 수 있음
    '''