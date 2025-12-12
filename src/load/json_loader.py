import os
import json
import logging

from config import path

logger = logging.getLogger(__name__)


class JsonLoader:
    def __init__(self):
        pass

    def salvar(self, conteudo: str, nome_arquivo: str, diretorio_destino: str = path.RAW_DATA_DIR) -> None:
        if not os.path.exists(diretorio_destino):
            raise FileNotFoundError(f"O diretório '{diretorio_destino}' não existe.")
        
        logger.info(f"Salvando arquivo JSON em: {diretorio_destino}")

        caminho_completo = os.path.join(diretorio_destino, f'{nome_arquivo}.json')
        with open(caminho_completo, 'w', encoding = 'utf-8') as arquivo:
            json.dump(conteudo, arquivo, ensure_ascii = False, indent = 4)
        logger.info(f"Arquivo JSON salvo com sucesso em: {caminho_completo}")