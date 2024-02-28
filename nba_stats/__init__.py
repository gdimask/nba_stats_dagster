from dagster import Definitions, load_assets_from_modules

from .assets import daily_player_stats, metrics
from .resources import bigquery_resource, parquet_io_manager
from .schedules import asset_partitioned_schedule
from .jobs import daily_job
import os
import json
import base64

AUTH_FILE = "gcp_creds.json"
with open(AUTH_FILE, "w") as f:
    json.dump(json.loads(base64.b64decode(os.getenv("GCP_ENV_B64"))), f)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = AUTH_FILE
print(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])

daily_player_stats_assets = load_assets_from_modules([daily_player_stats])
metric_assets = load_assets_from_modules([metrics])
all_jobs = [daily_job]
all_schedules = [asset_partitioned_schedule]

defs = Definitions(
    assets=[*daily_player_stats_assets, *metric_assets],
    resources={
        "bigquery": bigquery_resource,
        "parquet_io_manager": parquet_io_manager
    },
    jobs=all_jobs,
    schedules=all_schedules
)
