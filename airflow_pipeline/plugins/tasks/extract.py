import logging
from datetime import datetime

from airflow.decorators import task

from hooks.proposicoes_hook import ProposicoesHook
from hooks.datalake_hook import DatalakeHook
from transformers.proposicoes_transformer import ProposicoesTransformer

logger = logging.getLogger(__name__)

@task()
def extract_proposicoes(start_date: datetime, end_date: datetime) -> str:
    logger.info(f"Extracting proposições from {start_date} to {end_date}")
    http_hook = ProposicoesHook(start_date, end_date)
    response = http_hook.run()
    transformer = ProposicoesTransformer()
    proposicoes = transformer.xml_to_json(response)
    datalake_hook = DatalakeHook()
    path = f'bronze/{start_date.strftime("%Y%m%d")}_{end_date.strftime("%Y%m%d")}/proposicoes.json'
    datalake_hook.save_json(proposicoes, path)
    logger.info(f"Saved {len(proposicoes)} proposições to {path}")
    return path