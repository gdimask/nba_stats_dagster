from dagster import build_schedule_from_partitioned_job
from ..jobs import daily_job

asset_partitioned_schedule = build_schedule_from_partitioned_job(
    daily_job
)
