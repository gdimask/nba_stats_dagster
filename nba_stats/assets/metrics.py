import pandas as pd

from dagster import asset
from dagster_gcp import BigQueryResource
from google.cloud import bigquery as bq_lib


@asset(
    deps=['daily_player_stats'],
)
def player_stats_per_season(bigquery: BigQueryResource):
    """ Asset for Player stats per season """
    query = f"""
        select
            season_year,
            player_id,
            sum(fgm) as fgm,
            sum(fga) as fga,
            sum(fg3m) as fg3m,
            sum(fg3a) as fg3a,
            sum(ftm) as ftm,
            sum(fta) as fta,
            sum(ast) as ast,
            sum(oreb) as oreb,
            sum(dreb) as dreb,
            sum(reb) as reb,
            sum(stl) as stl,
            sum(tov) as tov,
        from `dbtbigquery-331216.nba_stats.daily_player_stats`
        group by 1,2
    """

    job_config = bq_lib.LoadJobConfig()
    job_config.write_disposition = bq_lib.WriteDisposition.WRITE_TRUNCATE

    with bigquery.get_client() as client:
        job = client.query(query)
        df = job.to_dataframe()
        materialize_job = client.load_table_from_dataframe(
            dataframe=df,
            destination='nba_stats.player_stats_per_season',
            job_config=job_config
        )

        materialize_job.result()
