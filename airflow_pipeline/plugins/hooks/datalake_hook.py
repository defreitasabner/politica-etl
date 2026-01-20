import json
from io import BytesIO
from typing import Union

from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd


class DatalakeHook(BaseHook):   
    def __init__(self, conn_id = 'datalake_default'):
        self.conn_id = conn_id
        self.s3_hook = S3Hook(aws_conn_id = conn_id)
        self.bucket_name = 'datalake'
    
    def save_parquet(self, df: pd.DataFrame, path: str):
        buffer = BytesIO()
        df.to_parquet(buffer, index = False)
        self.s3_hook.load_bytes(
            bytes_data = buffer.getvalue(),
            key = path,
            bucket_name = self.bucket_name,
            replace = True
        )
        self.log.info(f"Saved {len(df)} rows to {self.bucket_name}/{path}")
    
    def load_parquet(self, path: str) -> pd.DataFrame:
        obj = self.s3_hook.get_key(key = path, bucket_name = self.bucket_name)
        df = pd.read_parquet(BytesIO(obj.get()['Body'].read()))
        self.log.info(f"Loaded {len(df)} rows from {self.bucket_name}/{path}")
        return df
    
    def save_json(self, data: Union[dict, list], path: str):
        self.s3_hook.load_string(
            string_data = json.dumps(data, ensure_ascii = False),
            key = path,
            bucket_name = self.bucket_name,
            replace = True
        )
        self.log.info(f"Saved JSON to {self.bucket_name}/{path}")
    
    def load_json(self, path: str):
        obj = self.s3_hook.get_key(key = path, bucket_name = self.bucket_name)
        return json.loads(obj.get()['Body'].read())
    
    def list_files(self, prefix: str):
        return self.s3_hook.list_keys(bucket_name = self.bucket_name, prefix = prefix)