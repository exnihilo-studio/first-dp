import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


@task()
def hello() -> str:
    return "hello"


@task()
def world() -> str:
    return "world"


@task()
def hello_world(hello: str, world: str) -> None:
    print(f"{hello}, {world}!")


@dag(
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    description="Example DAG",
    schedule_interval=None,
    catchup=False,
    tags=["genesis", "example"],
    dag_id="hellow_world_dag",
)
def hellow_world_dag():
    print_hello_bash = BashOperator(
        task_id="print_hello_bash",
        bash_command="echo 'hello from bash'",
    )
    a = hello()
    b = world()
    print_hello_bash >> hello_world(b, a)


dag = hellow_world_dag()
