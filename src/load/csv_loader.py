import os
import logging

import pandas as pd

from config import path

logger = logging.getLogger(__name__)


class CsvLoader:
    
    def salvar(self, dataframe: pd.DataFrame, nome_arquivo: str, diretorio_destino: str = path.PROCESSED_DATA_DIR) -> None:
        if not os.path.exists(diretorio_destino):
            raise FileNotFoundError(f"O diretório '{diretorio_destino}' não existe.")
        
        logger.info(f"Salvando arquivo CSV em: {diretorio_destino}")

        caminho_completo = os.path.join(diretorio_destino, f'{nome_arquivo}.csv')
        dataframe.to_csv(caminho_completo, index=False, encoding='utf-8')
        logger.info(f"Arquivo CSV salvo com sucesso em: {caminho_completo}")