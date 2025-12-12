import argparse
from datetime import datetime, timedelta


class CLI:
    
    def __init__(self):
        self.__TIPO_PROPOSICAO_DEFAULT = 'PL'
        self.__TIPOS_PROPOSICAO_CHOICES = [self.__TIPO_PROPOSICAO_DEFAULT]
        self.__DATA_INICIAL_DEFAULT = (datetime.now() - timedelta(days = 30)).strftime('%d/%m/%Y')
        self.__DATA_FINAL_DEFAULT = datetime.now().strftime('%d/%m/%Y')

    def parse_args(self):
        parser = argparse.ArgumentParser(description = 'Executa pipelines de processamento de dados políticos.')

        parser.add_argument('-tp', '--tipo-proposicao', type = str, default = self.__TIPO_PROPOSICAO_DEFAULT, choices = self.__TIPOS_PROPOSICAO_CHOICES, help = 'Sigla do tipo da proposição.')

        parser.add_argument('-di', '--data-inicial', type = str, default = self.__DATA_INICIAL_DEFAULT, help = 'Data inicial para o filtro (formato: DD/MM/AAAA)')
        parser.add_argument('-df', '--data-final', type = str, default = self.__DATA_FINAL_DEFAULT, help = 'Data final para o filtro (formato: DD/MM/AAAA)')

        return parser.parse_args()