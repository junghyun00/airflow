from airflow.sdk import DAG, task
import datetime
import pendulum
# from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.branch import BaseBranchOperator
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id="dags_base_branch_operator_copy_2.py",
    schedule="10 0 * * *",  # 분 시 일 월 요일
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"), 
    catchup=False
) as dag:
    '''
    A가 선택되면 task_a 돌고
    B,C가 선택되면 task_b, task_c 돌고
    '''
    class CustomBranchOperator(BaseBranchOperator) : # BaseBranchOperator를 무조건 상속해야함   (CustomBranchOperator는 자식 클래스 / BaseBranchOperator는 부모 클래스)
        def choose_branch(self, context):   # BaseBranchOperator 클래스를 쓸거면 무조건 choose_branch(self, context):라는 함수를 써야함 (이름, 파라미터 바꾸면 안됨 디폴트임)
            import random
            print(context)   # context가 **kwargs 역할을 해줌

            item_list = ['A', 'B', 'C']
            selected_item = random.choice(item_list)
            if selected_item == 'A' :
                return 'task_a'
            elif selected_item in ['B', 'C'] :
                return ['task_b', 'task_c']

    # class는 설계도 > 해당 설계도를 custom_branch_operator라는 객체에 저장함
    custom_branch_operator = CustomBranchOperator(task_id = 'python_brach_task')


    '''
    여기서 어느 task들이 돌았는지 print로 찍어줌
    '''
    def common_fucn(**kwagrs):
        print(kwagrs.get('selected'))

    task_a = PythonOperator(
        task_id = 'task_a',
        python_callable=common_fucn,
        op_kwargs={'selected' : 'A'}
    )

    task_b = PythonOperator(
        task_id = 'task_b',
        python_callable=common_fucn,
        op_kwargs={'selected' : 'B'}
    )

    task_c = PythonOperator(
        task_id = 'task_c',
        python_callable=common_fucn,
        op_kwargs={'selected' : 'C'}
    )


    # 객체를 실행시킴
    custom_branch_operator >> [task_a, task_b, task_c]