"""EMR cluster support module."""

from typing import Sequence, Optional

from airflow.models.baseoperator import BaseOperator
# from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow_contrib_v0_2_12.operators.emr_create_stack import EmrCreateStackSyncOperator
from airflow_contrib_v0_2_12.operators.emr_terminate_stack import EmrTerminateStackOperator
from airflow_contrib_v0_2_12.operators.emr_add_steps import EmrAddStepsSyncOperator

from .pmi_dag import PMIDAG


class EmrCluster():
    """Pipeline's EMR cluster.

    This class encapsulates functionality for creating tasks that use the
    pipeline's EMR cluster.

    """

    EMR_CLUSTER_MAX_IDLE_HOURS = 2

    def __init__(self):

        self._dag = PMIDAG.get_dag()
        env_id = self._dag.env_id
        app_name = self._dag.app_name
        region_designator = self._dag.region_designator
        env_config = self._dag.env_config

        # create cluster start task
        self._start_emr_cluster_task = EmrCreateStackSyncOperator(
            task_id='start_emr_cluster',
            trigger_rule=TriggerRule.ONE_SUCCESS,
            region=self._dag.aws_env['region'],
            account=env_config['emr_cluster_account'],
            stack_name="{{ ti.dag_id }}-{{ ti.start_date.strftime('%Y%m%d%H') }}",
            resource_tags={
                'team': PMIDAG.TEAM,
                'project': app_name,
                'user': f'{PMIDAG.PRODUCT_NAME}-airflow-{env_id}'
            },
            job_flow_overrides={
                'service_role': f'{app_name}-{env_id}-{region_designator}-emr',
                'job_flow_role': f'{app_name}-{env_id}-{region_designator}-emr-job',
                'core_instance_type': env_config['emr_cluster_instance_type'],
                'core_instance_count': env_config['emr_cluster_instance_count'],
                'ec2_key_name': env_config.get('emr_cluster_ssh_keypair'),
                'additional_classification_props': [
                    {
                        'classification': 'spark-env',
                        'configurations': [
                            {
                                'classification': 'export',
                                'configuration_properties': {
                                    'PYSPARK_PYTHON': '/usr/bin/python3'
                                }
                            }
                        ]
                    }
                ]
            },
            alarm_topic_name=env_config['alarms_topic_name'],
            emr_idle_min_minutes=EmrCluster.EMR_CLUSTER_MAX_IDLE_HOURS * 60
        )

        # create cluster termination task
        self._terminate_emr_cluster_task = EmrTerminateStackOperator(
            task_id='terminate_emr_cluster',
            trigger_rule=TriggerRule.ALL_DONE,
            region=self._dag.aws_env['region'],
            stack_name=(
                f"{{{{ ti.xcom_pull(task_ids='{self._start_emr_cluster_task.task_id}', key='return_value')[0] }}}}"
            )
        )
        # NOTE: Developers may want to switch the EMR cluster termination to a
        #       dummy operator below so that they can see the EMR job logs. If
        #       you do it, don't forget to switch it back and to manually delete
        #       CloudFormation stack created for the EMR cluster.
        # self._terminate_emr_cluster_task = DummyOperator(
        #     task_id='terminate_emr_cluster',
        #     trigger_rule=TriggerRule.ALL_DONE
        # )


    @property
    def start_emr_cluster_task(self) -> BaseOperator:
        """Task used to start the EMR cluster."""
        return self._start_emr_cluster_task

    @property
    def terminate_emr_cluster_task(self) -> BaseOperator:
        """Task used to terminate the EMR cluster."""
        return self._terminate_emr_cluster_task

    def create_emr_job_task(
            self,
            *,
            task_id: str,
            job_name: str,
            script: str,
            script_args: Optional[Sequence[str]] = None
    ) -> BaseOperator:
        """Create EMR job task.

        Parameters
        ----------
        task_id
            Task id.
        job_name
            EMR job name.
        script
            The job script.
        script_args
            Optional job script arguments. Can contain templates.

        Returns
        -------
        BaseOperator
            The task.

        """

        job_flow_id = (
            f"{{{{ ti.xcom_pull(task_ids='{self._start_emr_cluster_task.task_id}', key='return_value')[1] }}}}"
        )

        assets_location = self._dag.assets_location
        command_runner_args = [
            'spark-submit',
            '--deploy-mode', 'cluster',
            '--master', 'yarn',
            '--packages', 'org.apache.spark:spark-avro_2.11:2.4.0',
            '--conf', 'spark.yarn.submit.waitAppCompletion=true',
            '--py-files', f'{assets_location}/emr/packages.zip',
            f'{assets_location}/emr/{script}'
        ]
        if script_args:
            command_runner_args.extend(script_args)

        return EmrAddStepsSyncOperator(
            task_id=task_id,
            region=self._dag.aws_env['region'],
            job_flow_id=job_flow_id,
            steps=[
                {
                    'Name': job_name,
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': command_runner_args
                    }
                }
            ]
        )
