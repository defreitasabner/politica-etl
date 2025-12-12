import os
import xml.dom.minidom
import logging

from config import path

logger = logging.getLogger(__name__)

class XmlLoader:
    def __init__(self):
        pass

    def salvar(self, conteudo: str, nome_arquivo: str, diretorio_destino: str = path.RAW_DATA_DIR) -> None:
        if not os.path.exists(diretorio_destino):
            raise FileNotFoundError(f"O diretório '{diretorio_destino}' não existe.")

        logger.info(f"Salvando arquivo XML em: {diretorio_destino}")
        
        dom = xml.dom.minidom.parseString(conteudo)
        xml_formatado = dom.toprettyxml(indent="  ", encoding="utf-8").decode('utf-8')
        caminho_completo = os.path.join(diretorio_destino, f'{nome_arquivo}.xml')
        with open(caminho_completo, 'w', encoding='utf-8') as arquivo:
            arquivo.write(xml_formatado)
        logger.info(f"Arquivo XML salvo com sucesso em: {caminho_completo}")