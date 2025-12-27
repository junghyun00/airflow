from airflow.sdk import DAG, task
import datetime
import pendulum
from airflow.providers.standard.operators.python import PythonOperator  # 함수를 실행시키는 오퍼레이터


with DAG(
    dag_id="wjh_sur_tag_mig_test", 
    schedule="30 6 * * *",  # 분 시 일 월 요일
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:

    # def insert_postgres(postgres_conn_id, **kwargs):
    #     from airflow.providers.postgres.hooks.postgres import PostgresHook
    #     from contextlib import closing

    #     postgres_hook = PostgresHook(postgres_conn_id)
    #     with closing(postgres_hook.get_conn()) as conn:
    #         with closing(conn.cursor()) as cursor:
    #             dag_id = kwargs.get('ti').dag_id
    #             task_id = kwargs.get('ti').task_id
    #             run_id = kwargs.get('ti').run_id
    #             msg = 'insert 수행'
    #             sql = 'insert into py_opr_drct_insert values (%s,%s,%s,%s);'
    #             cursor.execute(sql, (dag_id, task_id, run_id, msg))
    #             conn.commit()

    # insrt_postgres_task = PythonOperator(
    #     task_id = 'insrt_postgres_task',
    #     python_callable=insert_postgres,
    #     op_kwargs={'postgres_conn_id':'conn-target'},
    #     connect_timeout=5
    # )

    def mig_data(sur_conn_id, tag_conn_id, **kwargs):
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        sur_hook = PostgresHook(sur_conn_id)
        tag_hook = PostgresHook(tag_conn_id)
        
        rows = sur_hook.get_records(
            """
            select rntls_id, addr1, addr2 , lat, lot  from tb_bike_station_master
            """
        )

        tag_hook.run("TRUNCATE TABLE tb_bike_station_master")

        tag_hook.insert_rows(
            table = 'tb_bike_station_master',
            rows = rows,
            target_fields = ["rntls_id", "addr1", "addr2" , "lat", "lot" ],
            commit_every = 1
        )

    mig_task = PythonOperator(
        task_id = 'mig_task',
        python_callable=mig_data,
        op_kwargs={'sur_conn_id' : 'conn-db-postgrs-custom', 'tag_conn_id' : 'conn-target'}
    )