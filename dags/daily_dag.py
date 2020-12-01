"""Daily run pipeline DAG module."""

from datetime import datetime
from typing import Sequence, Mapping, Any

from airflow.operators.python_operator import BranchPythonOperator

from .common import PMIDAG, EmrCluster, BasePartnerPipeline
from .common.sensors import SourcePartitionsSensor
#from .pinterest import PinterestViewabilityPipeline, PinterestCampaignIdMappingsPipeline #commented by rohit
from .snapchat import SnapchatViewabilityPipeline, SnapchatCampaignIdMappingsPipeline


class DailyDAG(PMIDAG):
    """Daily run pipeline DAG.

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
            'pipeline-daily',
            description="PM daily run data processing pipeline.",
            app_name=app_name,
            env_config=env_config,
            schedule=env_config['airflow_daily_schedule'],
            start_date=start_date
        )

        # create DAG tasks
        with self:

            # create EMR cluster
            emr_cluster = EmrCluster()

            # partner pipelines
            partner_pipelines: Sequence[BasePartnerPipeline] = [
                #commented by rohit
                '''PinterestViewabilityPipeline(
                    emr_cluster=emr_cluster
                ),
                PinterestCampaignIdMappingsPipeline(
                    emr_cluster=emr_cluster
                ),'''
                SnapchatViewabilityPipeline(
                    emr_cluster=emr_cluster
                )
                SnapchatCampaignIdMappingsPipeline(
                    emr_cluster=emr_cluster
                ),
                # ADD MORE PARTNER PIPELINES HERE
            ]
            all_partners = set(pp.partner for pp in partner_pipelines)

            # input data availability sensors
            input_data_sensors = {
                partner: SourcePartitionsSensor(
                    task_id=f'{partner}_src_parts_sensor',
                    partner=partner
                ) for partner in all_partners
            }

            # partner pipeline branching
            def partner_pipeline_branch_func(**kwargs):
                """Determine partner pipelines to run."""
                conf = kwargs['dag_run'].conf or {}
                partners = set(conf.get('partners', all_partners))
                return [
                    input_data_sensors[partner].task_id for partner in partners
                ]

            partner_pipeline_branch_task = BranchPythonOperator(
                task_id='partner_pipeline_branch',
                python_callable=partner_pipeline_branch_func,
                provide_context=True
            )

        # connect DAG tasks:

        for input_data_sensor in input_data_sensors.values():
            input_data_sensor.set_upstream(partner_pipeline_branch_task)

        emr_cluster.start_emr_cluster_task.set_upstream(input_data_sensors.values())

        for partner_pipeline in partner_pipelines:
            partner_pipeline.entry_task.set_upstream([
                input_data_sensors[partner_pipeline.partner],
                emr_cluster.start_emr_cluster_task
            ])

        emr_cluster.terminate_emr_cluster_task.set_upstream([
            partner_pipeline.exit_task for partner_pipeline in partner_pipelines
        ])
