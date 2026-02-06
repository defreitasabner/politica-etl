import logging

from src.extract.camara import extract_proposicoes
from src.load.camara import salvar_dados_brutos, carregar_dados_brutos, salvar_dados_transformados
from src.transform.camara import transformar_proposicoes


logger = logging.getLogger(__name__)

PROPOSICOES_COLUNAS_INTERESSE = [
    'id', 'nome', 'numero', 'datApresentacao', 'txtEmenta', 'txtExplicacaoEmenta', 
    'autor1.txtNomeAutor', 'autor1.codPartido', 'autor1.txtSiglaPartido', 'autor1.txtSiglaUF',
]
PROPOSICOES_RENAME_COLUNAS = {
    'id': 'id_proposicao',
    'nome': 'nome_proposicao',
    'numero': 'numero_proposicao',
    'datApresentacao': 'data_apresentacao',
    'txtEmenta': 'ementa',
    'txtExplicacaoEmenta': 'explicacao_ementa',
    'autor1.txtNomeAutor': 'nome_autor_principal',
    'autor1.codPartido': 'codigo_partido_autor_principal',
    'autor1.txtSiglaPartido': 'sigla_partido_autor_principal',
    'autor1.txtSiglaUF': 'sigla_uf_autor_principal',
}

def proposicoes_etl(
    sigla_tipo_proposicao: list[str] = ['PL', 'PLP', 'PEC'], 
    data_apresentacao_inicio: str = '', 
    data_apresentacao_fim: str = ''
) -> None:
    logger.info('Iniciando pipeline de ETL para Proposições da Câmara dos Deputados.')
    dados, extract_params = extract_proposicoes(
        sigla_tipo_proposicao = sigla_tipo_proposicao,
        data_apresentacao_inicio = data_apresentacao_inicio,
        data_apresentacao_fim = data_apresentacao_fim
    )
    dados_filepath, metadados_filepath = salvar_dados_brutos(dados, extract_params)
    dados, metadados = carregar_dados_brutos(dados_filepath, metadados_filepath)
    dados, transform_params = transformar_proposicoes(
        proposicoes_json = dados, 
        selecionar_colunas = PROPOSICOES_COLUNAS_INTERESSE,
        renomear_colunas = PROPOSICOES_RENAME_COLUNAS
    )
    salvar_dados_transformados(
        dados_transformados = dados, 
        metadados_fonte = metadados, 
        transform_params = transform_params
    )
    logger.info('Pipeline de ETL para Proposições da Câmara dos Deputados concluída.')
