from airflow.hooks.base import BaseHook
from pymongo import MongoClient


class MongoHook(BaseHook):
    def __init__(self):
        self.conn_id = 'mongo_default'
        
    def get_conn(self):
        connection = self.get_connection(self.conn_id)
        client = MongoClient(connection.get_uri())
        return client
    
    def insert_many(self, database, collection, documents):
        client = self.get_conn()
        try:
            db = client[database]
            coll = db[collection]
            result = coll.insert_many(documents)
            return result.inserted_ids
        finally:
            client.close()