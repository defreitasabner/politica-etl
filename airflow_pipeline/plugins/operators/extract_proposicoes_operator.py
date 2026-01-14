from airflow.models import BaseOperator

from hooks.proposicoes_hook import ProposicoesHook
from transformers.proposicoes_transformer import ProposicoesTransformer


class ExtractProposicoesOperator(BaseOperator):
    def __init__(self, end_time, start_time, **kwargs):
        self.end_time = end_time
        self.start_time = start_time
        super().__init__(**kwargs)

    def execute(self, context):
        hook = ProposicoesHook(self.end_time, self.start_time)
        response = hook.run()
        return ProposicoesTransformer().xml_to_json(response)