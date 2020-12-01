"""Airflow pipeline DAGs.

See https://airflow.apache.org/docs/stable/ for details.

"""

# keywords to make Airflow pick up DAGs in this file: airflow DAG

from datetime import datetime
import json

import pkg_resources

from dags import DailyDAG, HourlyDAG, FacebookDAG


APP_NAME = 'etl-pm-pipeline-be-rgarde'

START_DATE = datetime(2020, 9, 10)


# load environment configuration
env_config = json.loads(pkg_resources.resource_string(
    __name__, 'env-config.json'
))

# create DAGs
daily_dag = DailyDAG(APP_NAME, env_config, START_DATE)
hourly_dag = HourlyDAG(APP_NAME, env_config, START_DATE)
facebook_dag = FacebookDAG(APP_NAME, env_config, START_DATE)
