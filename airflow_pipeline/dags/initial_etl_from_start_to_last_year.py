import pendulum
from airflow.decorators import dag
from tasks.extract import extract_proposicoes, extract_votacoes


START_YEAR_DEFAULT = 2013

@dag(
    dag_id = f'initial_etl_from_{START_YEAR_DEFAULT}_to_last_year',
    schedule_interval = None,
    tags = ['extract', 'seeding'],
    catchup = False,
)
def initial_etl_from_start_to_last_year():
    current_year = pendulum.now().year
    for year in range(START_YEAR_DEFAULT, current_year):
        extract_proposicoes.override(task_id = f'extracting_proposicoes_{year}')(
            start_date = pendulum.datetime(year, 1, 1),
            end_date = pendulum.datetime(year, 12, 31)
        )

initial_etl_from_start_to_last_year()
