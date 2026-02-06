import requests
from airflow.providers.http.hooks.http import HttpHook


class ProposicoesHook(HttpHook):
    def __init__(self, start_time, end_time):
        self.start_time = start_time
        self.end_time = end_time
        self.DATE_FORMAT = '%d/%m/%Y'
        
        self.conn_id = 'camara_api'
        super().__init__(http_conn_id = self.conn_id)

    def create_url(self):
        return f'{self.base_url}/SitCamaraWS/Proposicoes.asmx/ListarProposicoes'

    def connect(self, url, session):
        params = {
            'sigla': ['PL', 'PEC', 'PLP'],
            'numero': '',
            'ano': self.start_time.year,
            'datApresentacaoIni': self.start_time.strftime(self.DATE_FORMAT),
            'datApresentacaoFim': self.end_time.strftime(self.DATE_FORMAT),
            'IdTipoAutor': '',
            'ParteNomeAutor': '',
            'SiglaPartidoAutor': '',
            'SiglaUfAutor': '',
            'GeneroAutor': '',
            'IdSituacaoProposicao': '',
            'IdOrgaoSituacaoProposicao': '',
            'EmTramitacao': '',
            'codEstado': '',
            'codOrgaoEstado': ''
        }
        request = requests.Request('GET', url, params = params)
        prep = session.prepare_request(request)
        self.log.info(f'Connecting to: {prep.url}')
        return self.run_and_check(session, prep, {})
    
    def get_response(self, url, session):
        response = self.connect(url, session)
        return response.content

    def run(self):
        session = self.get_conn()
        url = self.create_url()
        return self.get_response(url, session) 