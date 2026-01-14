from airflow.models import BaseOperator

from hooks.mongo_hook import MongoHook


class LoadMongoOperator(BaseOperator):
    def __init__(self, database, collection, source_task_id = None, **kwargs):
        self.database = database
        self.collection = collection
        self.source_task_id = source_task_id
        super().__init__(**kwargs)

    def execute(self, context):
        ti = context['ti']
        data = ti.xcom_pull(task_ids = self.source_task_id)
        
        hook = MongoHook()
        inserted_ids = hook.insert_many(self.database, self.collection, data)
        
        self.log.info(f"Loaded {len(inserted_ids)} Proposições to MongoDB")
        return len(inserted_ids)