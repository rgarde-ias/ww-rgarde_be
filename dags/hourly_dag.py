"""Hourly run DAG module."""

from typing import Mapping, Any
from datetime import datetime

from .common import PMIDAG
from .pinterest import PinterestHourlyTasks
from .snapchat import SnapchatHourlyTasks


class HourlyDAG(PMIDAG):
    """Hourly run DAG.

    Parameters
    ----------
    app_name
        AWS application (stack) name.
    env_config
        Deployment environment configuration.
    start_date
        DAG start date.

    """

    def __init__(
            self,
            app_name: str,
            env_config: Mapping[str, Any],
            start_date: datetime
    ):
        super().__init__(
            'pipeline-hourly',
            description="PM hourly run tasks.",
            app_name=app_name,
            env_config=env_config,
            schedule=env_config['airflow_hourly_schedule'],
            start_date=start_date
        )

        # disable automatic backfill
        self.catchup = False

        # create DAG tasks
        with self:
            PinterestHourlyTasks()
            SnapchatHourlyTasks()
            # ADD MORE PARTNERS HERE
