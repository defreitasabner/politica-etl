import logging
from datetime import datetime

import requests

from src.common import parsers


logger = logging.getLogger(__name__)

BASE_URL = 'https://www.camara.leg.br/SitCamaraWS/Proposicoes.asmx'


def extract_proposicoes(
    sigla_tipo_proposicao: list[str] = [], numero_proposicao: str = '', ano_proposicao: str = '',
    data_apresentacao_inicio: str = '', data_apresentacao_fim: str = '', id_tipo_autor: str = '',
    parte_nome_autor: str = '', sigla_partido_autor: str = '', sigla_uf_autor: str = '', genero_autor: str = '',
    id_situacao_proposicao: str = '', id_orgao_situacao_proposicao: str = '',
    em_tramitacao: str = '', cod_estado: str = '', cod_orgao_estado: str = ''
) -> tuple[list[dict], dict]:
    url = f'{BASE_URL}/ListarProposicoes'
    logger.info('Iniciando extração de proposições utilizando o endpoint: %s', url)
    if not ano_proposicao:
        ano_proposicao = data_apresentacao_inicio.split('/')[-1]
    query_params = {
        'sigla': sigla_tipo_proposicao,
        'numero': numero_proposicao,
        'ano': ano_proposicao,
        'datApresentacaoIni': data_apresentacao_inicio,
        'datApresentacaoFim': data_apresentacao_fim,
        'IdTipoAutor': id_tipo_autor,
        'ParteNomeAutor': parte_nome_autor,
        'SiglaPartidoAutor': sigla_partido_autor,
        'SiglaUfAutor': sigla_uf_autor,
        'GeneroAutor': genero_autor,
        'IdSituacaoProposicao': id_situacao_proposicao,
        'IdOrgaoSituacaoProposicao': id_orgao_situacao_proposicao,
        'EmTramitacao': em_tramitacao,
        'codEstado': cod_estado,
        'codOrgaoEstado': cod_orgao_estado
    }
    response = requests.get(url, params = query_params)
    logger.info(f"Requisição GET para {response.url}")
    response.raise_for_status()
    extract_params = {
        'url': response.url,
        'params': {
            'siglas': sigla_tipo_proposicao,
            'ano': ano_proposicao,
            'data_inicio': datetime.strptime(data_apresentacao_inicio, '%d/%m/%Y').strftime('%Y-%m-%d'),
            'data_fim': datetime.strptime(data_apresentacao_fim, '%d/%m/%Y').strftime('%Y-%m-%d'),
        },
    }
    proposicoes_json = parsers.xml_to_dict(response.content)['proposicoes']['proposicao']
    logger.info(f'Foram extraídas {len(proposicoes_json)} proposições.')
    logger.info('Extração de proposições concluída.')
    return proposicoes_json, extract_params
