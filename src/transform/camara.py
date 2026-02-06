import logging

import pandas as pd


logger = logging.getLogger(__name__)


def transformar_proposicoes(
    proposicoes_json: list[dict], 
    selecionar_colunas: list[str] = [],
    renomear_colunas: dict = {},
    ) -> tuple[pd.DataFrame, dict]:
    logger.info('Iniciando transformação dos dados de proposições.')
    proposicoes = pd.json_normalize(proposicoes_json)
    previous_columns = proposicoes.columns.tolist()
    if selecionar_colunas:
        logger.info(f'Selecionando colunas: {selecionar_colunas}')
        proposicoes = proposicoes[selecionar_colunas].copy()
    if renomear_colunas:
        logger.info(f'Renomeando colunas: {renomear_colunas}')
        proposicoes = proposicoes.rename(columns = renomear_colunas)
    transform_params = {
        'source_columns': previous_columns,
        'columns_selected': selecionar_colunas,
        'columns_renamed': renomear_colunas,
    }
    logger.info('Transformação dos dados de proposições concluída.')
    return proposicoes, transform_params
