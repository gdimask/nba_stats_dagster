from nba_api.stats.endpoints import playergamelogs
from dagster import asset, EnvVar
from dagster_gcp import BigQueryResource
from . import constants
from ..partitions import daily_partition
from datetime import datetime


@asset(
    partitions_def=daily_partition,
    io_manager_key="parquet_io_manager"
)
def daily_player_stats_file(context):
    """ The raw parquet files for daily player stats. Sourced from NBA API"""
    date_to_fetch = context.asset_partition_key_for_output()
    season = make_season(date_to_fetch)
    formatted_date = format_date(date_to_fetch)
    username = EnvVar("PROXY_USERNAME").get_value()
    password = EnvVar("PROXY_PASSWORD").get_value()
    proxy =  f"https://{username}:{password}@us.smartproxy.com:10000"
    raw_df = playergamelogs.PlayerGameLogs(
        season_nullable = season,
        date_from_nullable = formatted_date,
        date_to_nullable = formatted_date,
        proxy=proxy
    ).player_game_logs.get_data_frame()

    return raw_df



@asset(
    deps=['daily_player_stats_file']
)
def daily_player_stats(bigquery: BigQueryResource):
    """ Asset for daily_player_stats table in BigQuery"""
    query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS `dbtbigquery-331216.nba_stats.daily_player_stats`
        WITH PARTITION COLUMNS
        (
            date_day date,
        )
        WITH CONNECTION `dbtbigquery-331216.us.nba-stats`
        OPTIONS (
        hive_partition_uri_prefix = "gs://nba-stats-grego/daily_player_stats/",
        uris=["gs://nba-stats-grego/daily_player_stats/*"],
        max_staleness = INTERVAL 24 HOUR,
        metadata_cache_mode = 'MANUAL',
        format ="PARQUET"
        );

        CALL BQ.REFRESH_EXTERNAL_METADATA_CACHE('dbtbigquery-331216.nba_stats.daily_player_stats');
    """

    with bigquery.get_client() as client:
        job = client.query(query)
        job.result()


def format_date(date_to_fetch: str) -> str:
    date_format = '%Y-%m-%d'
    date_obj = datetime.strptime(date_to_fetch, date_format)

    return date_obj.strftime('%m/%d/%Y')

def make_season(date_to_fetch: str) -> str:
    date_format = '%Y-%m-%d'
    date_obj = datetime.strptime(date_to_fetch, date_format)

    if(date_obj.month >= 7):
        year_end = int(date_obj.strftime('%y')) + 1
        season = f'{date_obj.year}-{year_end}'

    else:
        year_end = date_obj.strftime('%y')
        season = f'{date_obj.year - 1}-{year_end}'

    return season
