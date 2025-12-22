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
                  , 'file_nm' : {'/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/TbCorona19CountStatus.csv'}
                  }
    )