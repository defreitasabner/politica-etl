import argparse

from src.common import log
from src.pipelines import camara


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description = 'Extrai dados da Câmara dos Deputados.')
    subparsers = parser.add_subparsers(dest = 'comando', required = True, help = 'Comando a ser executado')

    proposicoes_parser = subparsers.add_parser('proposicoes', help = 'Extrai proposições da Câmara dos Deputados com base nos filtros fornecidos.')

    proposicoes_parser.add_argument('-di', '--data-inicio', type = str, required = True, help = 'Data inicial para o filtro (formato: DD/MM/AAAA)')
    proposicoes_parser.add_argument('-df', '--data-fim', type = str, required = True, help = 'Data final para o filtro (formato: DD/MM/AAAA)')

    args = parser.parse_args()

    log.setup_logging()
    if args.comando == 'proposicoes':
        camara.proposicoes_etl(
            data_apresentacao_inicio = args.data_inicio,
            data_apresentacao_fim = args.data_fim
        )
