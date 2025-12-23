from operators.seoul_api_to_csv_operator_up import SeoulApiToCsvOperator
import pendulum
from airflow.sdk import DAG


from airflow.providers.standard.operators.python import PythonOperator  # 함수를 실행시키는 오퍼레이터
from hooks.custom_postgres_hook import CustomPostgresHook  # 직접 만든 hook 호출 


with DAG(
    dag_id='wjh_api_test',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2025,4,1, tz='Asia/Seoul'), 
    catchup=False
) as dag:


    ''' 1. 행정동 단위 서울 생활인구(내국인) api로 csv 파일 만들기'''
    tb_seoul_people = SeoulApiToCsvOperator(
        task_id='tb_seoul_people',
        dataset_nm='SPOP_LOCAL_RESD_DONG',   # 이 데이터셋 이름은 꼭 api에 있는걸로 해야함 오퍼레이터를 그렇게 만들었음
        path='/opt/airflow/files/wjh_api_test/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='SPOP_LOCAL_RESD_DONG.csv',
        base_dt = '20251218'
    )


    ''' 2. DB에 INSERT 하기'''
    def insrt_postgre2(postgre_conn_id, tbl_nm, file_nm, **kwagrs):
        custom_postgre_hook = CustomPostgresHook(postgres_conn_id=postgre_conn_id)
        custom_postgre_hook.bulk_load(table_name=tbl_nm, file_name=file_nm, delimiter=',', is_header=True, is_replace=False)  # 해당 테이블이 있으면 기존 데이터 냅두고 append 함

    insrt_postgre_people = PythonOperator(
        task_id = 'insrt_postgre_people',
        python_callable=insrt_postgre2,
        op_kwargs={ 'postgre_conn_id': 'conn-db-postgrs-custom'
                  , 'tbl_nm' : 'tb_seoul_people'
                  , 'file_nm' : '/opt/airflow/files/wjh_api_test/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/SPOP_LOCAL_RESD_DONG.csv'
                  }

    )


    tb_seoul_people >> insrt_postgre_people