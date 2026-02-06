import logging
import os
from datetime import datetime

import pandas as pd

from src.load import datalake


logger = logging.getLogger(__name__)

SOURCE_SUBDIR = 'source=camara'
DATA_INICIO_SUBDIR = 'data_inicio={data_inicio}'
DATA_FIM_SUBDIR = 'data_fim={data_fim}'
TIMESTAMP_FORMAT = '%Y%m%d_%H%M%S'


def salvar_dados_brutos(dados_brutos: list[dict], extract_config: dict) -> tuple[str,str]:
    now = datetime.now()
    subdir_path = os.path.join(
        SOURCE_SUBDIR, 
        DATA_INICIO_SUBDIR.format(data_inicio = extract_config.get('params', {}).get('data_inicio', 'unknown')),
        DATA_FIM_SUBDIR.format(data_fim = extract_config.get('params', {}).get('data_fim', 'unknown'))
    )
    filename = f"proposicoes_{now.strftime(TIMESTAMP_FORMAT)}.json"
    dados_filepath = os.path.join(subdir_path, filename)
    datalake.save_to_bronze(dados_brutos, dados_filepath)
    logger.info(f"Dados brutos salvos na camada bronze: {dados_filepath}")
    metadados = {
        'extracted_at': now.isoformat(),
        'extract_config': extract_config,
        'record_count': len(dados_brutos),
        'filepath': dados_filepath,
    }
    metadados_filepath = os.path.join(subdir_path, f"proposicoes_{now.strftime(TIMESTAMP_FORMAT)}_metadata.json")
    datalake.save_to_bronze(metadados, metadados_filepath)
    logger.info(f"Metadados salvos na camada bronze: {metadados_filepath}")
    return dados_filepath, metadados_filepath


def carregar_dados_brutos(caminho_dados_brutos: str, caminho_metadados: str) -> tuple[str, dict]:
    dados_brutos = datalake.load_from_bronze(caminho_dados_brutos)
    metadados = datalake.load_from_bronze(caminho_metadados)
    logger.info(f"Dados brutos e metadados carregados da camada bronze: {caminho_dados_brutos}, {caminho_metadados}")
    return dados_brutos, metadados


def salvar_dados_transformados(dados_transformados: pd.DataFrame, metadados_fonte: dict, transform_params: dict) -> tuple[str, str]:
    now = datetime.now()
    subdir_path = os.path.join(
        SOURCE_SUBDIR, 
        DATA_INICIO_SUBDIR.format(data_inicio = metadados_fonte.get('extract_config', {}).get('params', {}).get('data_inicio', 'unknown')),
        DATA_FIM_SUBDIR.format(data_fim = metadados_fonte.get('extract_config', {}).get('params', {}).get('data_fim', 'unknown'))
    )
    filename = f"proposicoes_{now.strftime(TIMESTAMP_FORMAT)}.parquet"
    filepath = os.path.join(subdir_path, filename)
    datalake.save_to_silver(dados_transformados, filepath)
    logger.info(f"Dados transformados salvos na camada silver: {filepath}")
    
    metadados = {
        'transformed_at': now.isoformat(),
        'transform_params': transform_params,
        'source_metadata': metadados_fonte,
        'record_count': len(dados_transformados),
        'filepath': filepath,
    }
    metadados_filename = f"proposicoes_{now.strftime(TIMESTAMP_FORMAT)}_metadata.json"
    metadados_filepath = os.path.join(subdir_path, metadados_filename)
    datalake.save_to_silver(metadados, metadados_filepath)
    logger.info(f"Metadados salvos na camada silver: {metadados_filepath}")
    return filepath, metadados_filepath
