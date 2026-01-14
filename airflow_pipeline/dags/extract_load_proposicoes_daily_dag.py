from datetime import datetime

from airflow.models import DAG

from operators.extract_proposicoes_operator import ExtractProposicoesOperator
from operators.load_datalake import LoadMongoOperator


with DAG(
    dag_id = 'extract_load_proposicoes_daily',
    schedule_interval = '@daily',
    start_date = datetime(2026, 1, 1),
) as dag:
    
    EXTRACT_TASK_ID = 'extract_proposicoes'
    extract_task = ExtractProposicoesOperator(
        task_id = EXTRACT_TASK_ID,
        end_time = '{{ data_interval_end }}',
        start_time = '{{ data_interval_start }}',
    )

    LOAD_TASK_ID = 'load_proposicoes'
    load_task = LoadMongoOperator(
        task_id = LOAD_TASK_ID,
        database = 'politica',
        collection = 'proposicoes',
        source_task_id = EXTRACT_TASK_ID,
    )

    extract_task >> load_task