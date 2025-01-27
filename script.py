from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from pendulum import datetime

@dag(
    start_date=datetime(2021, 1, 1),
    schedule='@daily',
    catchup=False,
)

def taskflow_dag():
    @task
    def my_task_1():
        import time
        time.sleep(5)
        print("Hello from task 1")
    
    @task
    def my_task_2():
        print("Hello from task 2")
    
    chain(my_task_1(), my_task_2())

taskflow_dag()

