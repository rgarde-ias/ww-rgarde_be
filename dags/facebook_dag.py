"""Facebook pipeline DAG module."""

from typing import Mapping, Any
from datetime import datetime

from .common import PMIDAG, EmrCluster
from .common.sensors import SourcePartitionsSensor
from .facebook import FacebookViewabilityPipeline


class FacebookDAG(PMIDAG):
    """Facebook pipeline DAG.

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
            start_date: datetime,
    ):
        super().__init__(
            'pipeline-facebook',
            description="Facebook data processing pipeline.",
            app_name=app_name,
            env_config=env_config,
            schedule=env_config['partners']['facebook']['airflow_schedule'],
            start_date=start_date,
        )

        # create DAG tasks
        with self:

            # create EMR cluster
            emr_cluster = EmrCluster()

            facebook_viewability_pipeline = FacebookViewabilityPipeline(
                emr_cluster=emr_cluster
            )
            partner = facebook_viewability_pipeline.partner
            facebook_input_data_sensor = SourcePartitionsSensor(
                task_id=f'{partner}_src_parts_sensor',
                partner=partner,
            )

        # connect DAG tasks:

        emr_cluster.start_emr_cluster_task.set_upstream(facebook_input_data_sensor)

        facebook_viewability_pipeline.entry_task.set_upstream([
            facebook_input_data_sensor,
            emr_cluster.start_emr_cluster_task,
        ])

        emr_cluster.terminate_emr_cluster_task.set_upstream([
            facebook_viewability_pipeline.exit_task,
        ])
