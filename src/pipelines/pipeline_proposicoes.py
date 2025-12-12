import logging

from extract import CamaraApi
from transform import TransformerProposicoes
from load import JsonLoader, XmlLoader, CsvLoader


logger = logging.getLogger(__name__)


class PipelineProposicoes:
    def __init__(self):
        self.__extractor = CamaraApi()
        self.__transformer = TransformerProposicoes()
    
    def executar(self, tipo_proposicao, data_inicial, data_final) -> None:
        logger.info('Iniciando execução do pipeline de proposições.')
        logger.info(f'Parâmetros recebidos - Tipo Proposição: {tipo_proposicao}, Data Inicial: {data_inicial}, Data Final: {data_final}')
        ano = data_inicial.split('/')[-1]
        xml_proposicoes = self.__extractor.listar_proposicoes(
            sigla_tipo_proposicao = tipo_proposicao,
            ano_proposicao = ano,
            data_apresentacao_inicio = data_inicial,
            data_apresentacao_fim = data_final
        )

        data_inicial_formatada = data_inicial.replace('/', '-')
        data_final_formatada = data_final.replace('/', '-')
        nome_arquivo = f'proposicoes_{data_inicial_formatada}_a_{data_final_formatada}'

        XmlLoader().salvar(xml_proposicoes, nome_arquivo)
        
        proposicoes_json = self.__transformer.xml_to_json(xml_proposicoes)
        
        JsonLoader().salvar(proposicoes_json, nome_arquivo)
        df = self.__transformer.json_to_dataframe(proposicoes_json)

        CsvLoader().salvar(df, nome_arquivo)
        
