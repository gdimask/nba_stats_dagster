from dagster import (
    IOManager,
    EnvVar,
    _check
)
from google.cloud import storage

import pandas as pd

class GCSParquetIOManager(IOManager):
    def __init__(self, bucket_name, client=None, prefix='pl_holder') -> None:
        self.bucket_name = _check.str_param(bucket_name, 'bucket_name')
        self.client = client or storage.Client(project=EnvVar("BIGQUERY_PROJECT_ID"), credentials="gcp_creds.json")
        self.bucket_obj = self.client.bucket(bucket_name)
        self.prefix = _check.str_param(prefix, 'prefix')

    def _get_file_name(self, context):
        # run_id, step_key, name = context.get_run_scoped_output_identifier()
        date_to_fetch = context.asset_partition_key

        return f"daily_stats_{date_to_fetch}"

    def _uri_for_key(self, context) -> str:
        date_to_fetch = context.asset_partition_key
        filename = self._get_file_name(context)

        return f"gcs://{self.bucket_name}/{self.prefix}/date_day={date_to_fetch}/{filename}.parquet"

    def handle_output(self, context, obj) -> None:
        key = self._uri_for_key(context)

        print(key)
        obj.to_parquet(key, index=False)

    def load_input(self, context):
        df = pd.read_csv(self._uri_for_key(context))

        return df
