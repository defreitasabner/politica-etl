import requests
from airflow.providers.http.hooks.http import HttpHook


class ProposicoesVotadasHook(HttpHook):
    def __init__(self, year):
        self.year = year
        
        self.conn_id = 'camara_api'
        super().__init__(http_conn_id = self.conn_id)

    def create_url(self):
        return f'{self.base_url}/SitCamaraWS/Proposicoes.asmx/ListarProposicoesVotadasEmPlenario'

    def connect(self, url, session):
        params = {
            'ano': self.year,
            'tipo': ''
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