from datetime import datetime

from airflow.decorators import dag

from tasks.extract import extract_proposicoes


START_YEAR_DEFAULT = 2020

@dag(
    dag_id = 'initial_etl_from_start_to_current_year',
    schedule_interval = None,
    tags = ['extract', 'seeding'],
    params = {'start_year': START_YEAR_DEFAULT},
    catchup = False,
)
def initial_etl_from_start_to_current_year():
    start_year = START_YEAR_DEFAULT
    current_year = datetime.now().year

    for year in range(start_year, current_year + 1):
        extract_proposicoes.override(task_id = f'extracting_proposicoes_{year}')(
            start_date = datetime(year, 1, 1),
            end_date = datetime(year, 12, 31)
        )

initial_etl_from_start_to_current_year()
