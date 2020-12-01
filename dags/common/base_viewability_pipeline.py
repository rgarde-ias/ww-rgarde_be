"""Base class for viewability pipeline implementations module."""

from airflow.utils.helpers import chain

from .base_partner_pipeline import BasePartnerPipeline
from .emr_cluster import EmrCluster
from .pmi_dag import PMIDAG
from .operators import (
    CreateDestinationPartitionsOperator,
    ClearDestinationBucketOperator,
    DataLoaderTriggerOperator
)


class BaseViewabilityPipeline(BasePartnerPipeline):
    """Base class for viewability pipeline implementations.

    Parameters
    ----------
    partner
        The partner.
    emr_cluster
        The EMR cluster.
    emr_jas_script
        Python script name for EMR JAS process.
    emr_mart_script
        Python script name for EMR marting process.

    """

    def __init__(
            self,
            *,
            partner: str,
            emr_cluster: EmrCluster,
            emr_jas_script: str,
            emr_mart_script: str,
    ):
        super().__init__(partner)

        dag = PMIDAG.get_dag()
        env_config = dag.env_config

        # erase existing JAS data
        erase_jas_data_task = ClearDestinationBucketOperator(
            task_id=f'{self.partner}_erase_existing_jas',
            bucket_name=env_config['jas_bucket_name'],
            bucket_data_prefix=env_config['jas_bucket_data_prefix'],
            partner=self.partner
        )

        # perform JAS aggregation
        jas_aggregation_task = emr_cluster.create_emr_job_task(
            task_id=f'{self.partner}_jas_aggregation',
            job_name=f'JAS Aggregation: {self.partner}',
            script=emr_jas_script,
            script_args=[
                '--data-date', '{{ ds }}',
                '--data-date-tz-offset', '{{ ds | data_date_tz_offset }}',
                '--input-database-name', env_config['raw_events_glue_db_name'],
                '--output-bucket-name', env_config['jas_bucket_name'],
                '--output-bucket-data-prefix', env_config['jas_bucket_data_prefix']
            ]
        )

        # create JAS partitions
        jas_partitions_task = CreateDestinationPartitionsOperator(
            task_id=f'{self.partner}_jas_partitions',
            database_name=env_config['jas_glue_db_name'],
            partner=self.partner,
            by_hour=True
        )

        # erase existing JAS mart data
        erase_jas_mart_data_task = ClearDestinationBucketOperator(
            task_id=f'{self.partner}_erase_existing_jas_mart',
            bucket_name=env_config['jas_mart_bucket_name'],
            bucket_data_prefix=env_config['jas_mart_bucket_data_prefix'],
            partner=self.partner
        )

        # perform JAS mart aggregation
        jas_mart_aggregation_task = emr_cluster.create_emr_job_task(
            task_id=f'{self.partner}_jas_mart_aggregation',
            job_name=f'JAS Mart Aggregation: {self.partner}',
            script=emr_mart_script,
            script_args=[
                '--partner', self.partner,
                '--data-date', '{{ ds }}',
                '--input-database-name', env_config['jas_glue_db_name'],
                '--output-bucket-name', env_config['jas_mart_bucket_name'],
                '--output-bucket-data-prefix', env_config['jas_mart_bucket_data_prefix']
            ]
        )

        # create JAS mart partitions
        jas_mart_partitions_task = CreateDestinationPartitionsOperator(
            task_id=f'{self.partner}_jas_mart_partitions',
            database_name=env_config['jas_mart_glue_db_name'],
            partner=self.partner,
            by_hour=False
        )

        # trigger data load
        trigger_data_load_task = DataLoaderTriggerOperator(
            task_id=f'{self.partner}_trigger_data_load',
            partner=self.partner
        )

        # connect tasks
        chain(
            erase_jas_data_task, jas_aggregation_task, jas_partitions_task,
            erase_jas_mart_data_task, jas_mart_aggregation_task, jas_mart_partitions_task,
            trigger_data_load_task
        )

        # set pipeline entry and exit
        self._entry_task = erase_jas_data_task
        self._exit_task = trigger_data_load_task

    @property
    def entry_task(self):
        return self._entry_task

    @property
    def exit_task(self):
        return self._exit_task
