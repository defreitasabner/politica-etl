import pymongo


class MongoLoader:
    def __init__(self, uri: str, database: str, collection: str):
        self.client = pymongo.MongoClient(uri)
        self.db = self.client[database]
        self.collection = self.db[collection]

    def load_data(self, data: dict):
        result = self.collection.insert_one(data)
        return result.inserted_id

    def load_multiple_data(self, data_list: list):
        result = self.collection.insert_many(data_list)
        return result.inserted_ids

    def find_data(self, query: dict):
        return list(self.collection.find(query))

    def close_connection(self):
        self.client.close()