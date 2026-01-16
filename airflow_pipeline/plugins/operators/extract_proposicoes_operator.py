from datetime import timedelta

from airflow.models import BaseOperator

from hooks.proposicoes_hook import ProposicoesHook
from transformers.proposicoes_transformer import ProposicoesTransformer
from hooks.mongo_hook import MongoHook


class ExtractProposicoesOperator(BaseOperator):

    def __init__(self, database, collection, **kwargs):
        self.database = database
        self.collection = collection
        super().__init__(**kwargs)

    def execute(self, context):
        start_time = context['data_interval_start']
        end_time = context['data_interval_end'] - timedelta(days = 1)
        http_hook = ProposicoesHook(start_time, end_time)
        response = http_hook.run()
        proposicoes = ProposicoesTransformer().xml_to_json(response)
        mongo_hook = MongoHook()
        inserted_ids = mongo_hook.insert_many(self.database, self.collection, proposicoes)
        self.log.info(f"Loaded {len(inserted_ids)} Proposições to MongoDB")