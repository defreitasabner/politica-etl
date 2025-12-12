import logging

from cli import CLI
from pipelines import PipelineProposicoes


logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s - %(name)s [%(levelname)s] %(message)s'
)

def main():
    args = CLI().parse_args()
    PipelineProposicoes().executar(args.tipo_proposicao, args.data_inicial, args.data_final)

if __name__ == '__main__':
    main()