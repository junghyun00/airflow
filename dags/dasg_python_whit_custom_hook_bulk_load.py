from airflow.sdk import DAG, task
import datetime
import pendulum
from airflow.providers.standard.operators.python import PythonOperator  # 함수를 실행시키는 오퍼레이터
from hooks.custom_postgrs_hook import CustomPostgresHook  # 직접 만든 hook 호출


with DAG(
    dag_id="dasg_python_whit_custom_hook_bulk_load", 
    schedule="30 6 * * *",  # 분 시 일 월 요일
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    def insrt_postgre(postgre_conn_id, tbl_nm, file_nm, **kwagrs):
        custom_postgre_hook = CustomPostgresHook(postgres_conn_id=postgre_conn_id)
        custom_postgre_hook.bulk_load(table_name=tbl_nm, file_nm=file_nm, delimiter='', is_header=True, is_replace=True)

    insrt_postgre = PythonOperator(
        task_id = 'insrt_postgre',
        python_callable=insrt_postgre,
        op_kwargs={ 'postgre_conn_id': 'conn-db-postgrs-custom'
                  , 'tbl_nm' : 'tb_bulk2'
                  , 'file_nm' : {'/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/TbCorona19CountStatus.csv'}
                  }

    )