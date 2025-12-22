from airflow.hooks.base import BaseHook
import psycopg2
import pandas as pd

class CustomPostgresHook(BaseHook):

    def __init__(self, postgres_conn_id, **kwagrs):
        self.postgres_conn_id = postgres_conn_id

    def get_conn(self):
        airflow_conn = BaseHook.get_connection(self.postgres_conn_id)   # BaseHook이 가진 get_connection라는 메서드를 사용해서 BaseHook을 객체로 담지 않아도 커넥션 정보를 받아올 수 있음
        self.host = airflow_conn.host
        self.user = airflow_conn.login
        self.password = airflow_conn.password
        self.dbname = airflow_conn.schema
        self.port = airflow_conn.port

        self.postgres_conn = psycopg2.connect(host = self.host, user= self.user, password = self.password, dbname=self.dbname, port = self.port)  # 커넥션 정보들로 세션을 연결함
        return self.postgres_conn  # 세션이 리턴됨
        # airflow_conn과 self.postgres_conn는 전혀 다른거임 (앞에거는 커넥션 정보가 담긴 객체, 뒤에건 세션이 담긴 객체)
    

    def bulk_load(self, table_name, delimiter: str, is_header: bool, is_replace: bool):
        from sqlalchemy import create_engine

        self.log.info('적재 대상 파일:' + file_name)
        self.log.info('테이블:' + table_name)
        self.get_conn()   # 세션 실행
        header = 0 if is_header else None                                      # is_header = True 면 0 False면 None
        if_exists = 'replace' if is_replace else 'append'                      # is_replace = True 면 0 False면 'append'
        file_df = pd.read_csv(file_name, header=header, delimiter=delimiter)

        for col in file_df.columns:   # 줄넘김 및 특수문자 제거
            try:
                # string 문자열이 아닐 경우 continue
                file_df[col] = file_df[col].str.replace('\r\n', '')   # file_df에 숫자형컬럼이 있을 수 있는데 이 str.replace 함수를 만나면 에러가 나니까 except으로 계속 실행하게끔 만듦
                self.log.info(f'{table_name}.{col}: 개행문자 제거')
            except:
                continue

        self.log.info('적재 건수:' + str(len(file_df)))
        uri = f'postgresql://{self.user}:{self.password}@{self.host}/{self.dbname}'
        engine = create_engine(uri)
        file_df.to_sql( name=table_name
                      , con=engine
                      , schema='public'
                      , if_exists=if_exists   # replace 아니면 append
                      , index = False  # df의 index는 올리지 말라
                      )