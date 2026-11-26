
from datetime import datetime

from airflow.sdk import DAG, task


with DAG(
    dag_id="my_first_dag",
    start_date=datetime(2026, 11, 22),
    schedule=None,
    catchup=False,
) as dag:

    @task
    def say_hello():
        print("Hello Data Engineering!")

    @task
    def calculate_sum():
        a = 10
        b = 20
        result = a + b

        print("The sum is:", result)

    task1 = say_hello()
    task2 = calculate_sum()

    task1 >> task2


