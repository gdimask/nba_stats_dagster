from dagster_gcp import BigQueryResource, GCSResource
from dagster import EnvVar
from .gcs_parquet_io_manager import GCSParquetIOManager

bigquery_resource = BigQueryResource(
    project=EnvVar("BIGQUERY_PROJECT_ID"),
)

gcs_resource = GCSResource(
    project=EnvVar("BIGQUERY_PROJECT_ID"),
)

parquet_io_manager = GCSParquetIOManager(
    bucket_name='nba-stats-grego',
    prefix='daily_player_stats',
)
