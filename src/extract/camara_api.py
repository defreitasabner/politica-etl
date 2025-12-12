import logging

import requests

logger = logging.getLogger(__name__)


class CamaraApi:
    def __init__(self):
        self.__BASE_URL = 'https://www.camara.leg.br/SitCamaraWS/Proposicoes.asmx'

    def listar_proposicoes(self, sigla_tipo_proposicao: str = '', numero_proposicao: str = '', ano_proposicao: str = '',
        data_apresentacao_inicio: str = '', data_apresentacao_fim: str = '', id_tipo_autor: str = '',
        parte_nome_autor: str = '', sigla_partido_autor: str = '', sigla_uf_autor: str = '', genero_autor: str = '',
        id_situacao_proposicao: str = '', id_orgao_situacao_proposicao: str = '',
        em_tramitacao: str = '', cod_estado: str = '', cod_orgao_estado: str = ''
    ) -> str:
        url = f"{self.__BASE_URL}/ListarProposicoes"
        params = {
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
        response = requests.get(url, params = params)
        logger.info(f"Requisição GET para {response.url} retornou status {response.status_code}.")
        response.raise_for_status()
        return response.content