from airflow.sdk import DAG, task
import datetime
import pendulum
from airflow.providers.standard.operators.python import PythonOperator  # 함수를 실행시키는 오퍼레이터


with DAG(
    dag_id="dags_python_with_postgrs_hook", 
    schedule="30 6 * * *",  # 분 시 일 월 요일
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:

    def insert_postgres(postgres_conn_id, **kwargs):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from contextlib import closing

        postgres_hook = PostgresHook(postgres_conn_id)
        with closing(postgres_hook.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg = 'hook insert 수행'
                sql = 'insert into public.py_opr_drct_insert values (%s,%s,%s,%s);'
                cursor.execute(sql, (dag_id, task_id, run_id, msg))
                conn.commit()

    insrt_postgres_with_hook = PythonOperator(
        task_id = 'insrt_postgres_with_hook',
        python_callable=insert_postgres,
        op_kwargs={'postgres_conn_id':'conn-db-postgrs-custom'}
    )