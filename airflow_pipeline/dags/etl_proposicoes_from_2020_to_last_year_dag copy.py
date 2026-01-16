from datetime import datetime

from airflow.models import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.models import DagRun

from operators.extract_proposicoes_operator import ExtractProposicoesOperator


def should_run(**context):
    dag_id = context['dag'].dag_id
    dag_runs = DagRun.find(dag_id = dag_id, state='success')
    return len(dag_runs) == 0

with DAG(
    dag_id = 'etl_proposicoes_from_2020_to_last_year',
    schedule_interval = '@yearly',
    start_date = datetime(2020, 1, 1),
) as dag:
    
    check_should_run = ShortCircuitOperator(
        task_id = 'check_should_run',
        python_callable = should_run,
    )
    
    EXTRACT_TASK_ID = 'extract_proposicoes'
    extract_task = ExtractProposicoesOperator(
        task_id = EXTRACT_TASK_ID,
        database = 'raw',
        collection = 'proposicoes'
    )

    check_should_run >> extract_task