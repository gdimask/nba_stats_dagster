from dagster import define_asset_job, AssetSelection
from ..partitions import daily_partition

daily_assets = AssetSelection.keys('daily_player_stats_file', 'daily_player_stats', 'player_stats_per_season')

daily_job = define_asset_job(
    name='daily_job',
    selection=daily_assets,
    partitions_def=daily_partition
)
