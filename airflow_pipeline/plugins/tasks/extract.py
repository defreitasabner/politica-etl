import logging
from datetime import datetime

from airflow.decorators import task

from hooks.proposicoes_hook import ProposicoesHook
from hooks.datalake_hook import DatalakeHook
from hooks.votacao_proposicao_hook import VotacoesProposicaoHook
from transformers.proposicoes_transformer import ProposicoesTransformer
from transformers.votacoes_transformer import VotacoesTransformer


logger = logging.getLogger(__name__)

@task()
def extract_proposicoes(start_date: datetime, end_date: datetime) -> str:
    logger.info(f"Extracting proposições from {start_date} to {end_date}")
    http_hook = ProposicoesHook(start_date, end_date)
    response = http_hook.run()
    proposicoes = ProposicoesTransformer().xml_to_json(response)
    datalake_hook = DatalakeHook()
    path = f'bronze/{start_date.strftime("%Y%m%d")}_{end_date.strftime("%Y%m%d")}/proposicoes.json'
    datalake_hook.save_json(proposicoes, path)
    logger.info(f"Saved {len(proposicoes)} proposições to {path}")
    return path

@task()
def extract_votacoes(proposicoes_bronze_path: str, start_date: datetime, end_date: datetime) -> str:
    logger.info(f"Extracting votações for proposições in year {start_date.year}")

    datalake_hook = DatalakeHook()
    proposicoes_metadata = datalake_hook.load_json(proposicoes_bronze_path)

    votacoes = []
    for proposicao in proposicoes_metadata:
        try:
            votacao_hook = VotacoesProposicaoHook(proposicao['numero'], proposicao['tipo'], start_date.year)
            votacao_response = votacao_hook.run()
            votacao_data = VotacoesTransformer().xml_to_json(votacao_response)
            votacoes.append(votacao_data)
        except Exception as e:
            logger.error(f"Error extracting votação for proposição {proposicao['tipo']} {proposicao['numero']}: {e}")

    datalake_hook = DatalakeHook()
    path = f'bronze/{start_date.strftime("%Y%m%d")}_{end_date.strftime("%Y%m%d")}/votacoes_proposicoes.json'
    datalake_hook.save_json(votacoes, path)
    logger.info(f"Saved votações for {len(votacoes)} proposições to {path}")
    return path