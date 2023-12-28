from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

host_fastapi = Variable.get("host_fastapi")
port_fastapi = Variable.get("port_fastapi")
endpoint = "albums"

date = "{{ (execution_date + macros.timedelta(hours=33)).strftime('%Y-%m-%d') }}"

default_args = {
    'owner': 'hooniegit',
    'depends_on_past': True,
    'start_date': datetime(2023,12,27)
}

dag = DAG(
    f"json_{endpoint}",
	default_args=default_args,
	tags=['spotify', 'load', 'json', endpoint],
	max_active_runs=1,
	schedule_interval="0 23 * * *")

start = EmptyOperator(
	task_id = 'start',
	dag=dag
)

curl = BashOperator(
    task_id="curl",
    bash_command=f"curl 'http://{host_fastapi}:{port_fastapi}/json/{endpoint}?insert_date={date}'",
    dag=dag
)

send_noti = BashOperator(
    task_id='send.noti',
    bash_command=f"""
    curl -X POST -H 'Authorization: Bearer imq0ABNavwxOZyYBYRJ6kFivrLcW2vwaUjK1sBtj4AY' \
    -F 'message= json/{endpoint} DAG {date} 스케줄 동작 중 오류 발생' \
    https://notify-api.line.me/api/notify
    """,
    dag=dag,
	trigger_rule='one_failed'
)

finish = EmptyOperator(
	task_id = 'finish',
	dag = dag,
	trigger_rule='all_done'
)

start >> curl >> [send_noti, finish]
