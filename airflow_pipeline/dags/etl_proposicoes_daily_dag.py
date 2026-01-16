from datetime import datetime

from airflow.models import DAG

from operators.extract_proposicoes_operator import ExtractProposicoesOperator


def first_day_of_current_year():
    now = datetime.now()
    return datetime(now.year, 1, 1)

with DAG(
    dag_id = 'etl_proposicoes_daily',
    schedule_interval = '@daily',
    start_date = first_day_of_current_year(),
) as dag:
    
    EXTRACT_TASK_ID = 'extract_proposicoes'
    extract_task = ExtractProposicoesOperator(
        task_id = EXTRACT_TASK_ID,
        database = 'raw',
        collection = 'proposicoes'
    )
