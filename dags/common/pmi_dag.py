"""Base PMI DAG module."""

from typing import Mapping, Any, Union
from abc import ABC
from datetime import datetime

import requests
import dateutil.tz

from airflow import settings
from airflow.models import DAG, TaskInstance
from airflow.contrib.operators.sns_publish_operator import SnsPublishOperator


def _failure_callback(context: Mapping[str, Any]):
    """Callback for when a task fails."""

    dag: PMIDAG = context['dag']
    task_instance: TaskInstance = context['ti']

    return SnsPublishOperator(
        task_id='task_failure_callback',
        aws_conn_id=dag.aws_conn_id,
        target_arn=':'.join([
            "arn:aws:sns",
            dag.aws_env['region'],
            dag.aws_env['accountId'],
            dag.env_config['alarms_topic_name']
        ]),
        subject='PMI DAG run failure',
        message=(
            f"Airflow task failure:\n"
            f"\n"
            f"\nDAG:             {task_instance.dag_id}"
            f"\nTask:            {task_instance.task_id}"
            f"\nExecution Date:  {task_instance.execution_date}"
            f"\nHost:            {task_instance.hostname}"
            f"\nError:           {context['exception']}"
        )
    ).execute(context)


def _data_date_tz_offset_filter(data_date: str) -> int:
    """Jinja2 filter implementation for getting timezone offset in seconds for the given data date."""

    return round(PMIDAG.DATA_DATE_TZ.utcoffset(datetime(
        *(int(p) for p in data_date.split('-'))
    )).total_seconds())


class PMIDAG(ABC, DAG):
    """Abstract PMI DAG.

    Parameters
    ----------
    pipeline_name
        Name of the pipeline represented by the DAG.
    description
        DAG description.
    app_name
        Project's AWS app (stack) name.
    env_config
        Deployment environment configuration. At the minimum, the following
        properties must be provided:

        * `env_id` - Deployment environment id ("dev", "staging", "prod", etc.).
        * `deployment_version` - DAG deployment version (e.g. "1.2.3").
        * `airflow_cluster_id` - Airflow cluster id.
        * `airflow_aws_conn_id` - Airflow AWS connection id (e.g. "aws_default").
        * `alarms_topic_name` - Name of the SNS topic to use for critical alarms,
          such as task failures.

    schedule
        DAG run schedule. May be `None`.
    start_date
        DAG start date.

    """

    PRODUCT_DOMAIN = 'etl'

    PRODUCT_NAME = 'pm'

    TEAM = 'weedwackers'

    DATA_DATE_TZ = dateutil.tz.gettz('America/New_York')

    @staticmethod
    def get_dag(**kwargs) -> 'PMIDAG':
        """Get the DAG (either from the provided arguments or the context)."""
        return kwargs.get('dag', settings.CONTEXT_MANAGER_DAG)


    def __init__(
            self,
            pipeline_name: str,
            *,
            description: str,
            app_name: str,
            env_config: Mapping[str, Any],
            schedule: Union[str, None],
            start_date: datetime
    ):
        ABC.__init__(self)

        self._pipeline_name = pipeline_name
        self._app_name = app_name
        self._env_config = env_config
        env_id = env_config['env_id']

        # get AWS environment configuration from the Airflow instance
        self._aws_env = requests.get(
            'http://169.254.169.254/latest/dynamic/instance-identity/document'
        ).json()
        self._region_designator = ''.join(part[0] for part in self._aws_env['region'].split('-'))

        # DAG assets location
        self._assets_location = (
            f"s3://iasrf-vcs-mixed-{self._region_designator}-de-{env_id}"
            f"/{PMIDAG.PRODUCT_NAME}/airflow/{app_name}/v{env_config['deployment_version']}"
        )

        # default operator arguments
        default_args = {
            'owner': PMIDAG.TEAM,
            'queue': f"{env_config['airflow_cluster_id']}.{env_id}",
            'start_date': start_date,
            'depends_on_past': False,
            'retries': 0,
            'email_on_failure': False,
            'email_on_retry': False,
            'on_failure_callback': _failure_callback
        }

        # initialize the DAG
        deployment_version_major = env_config['deployment_version'].split('.')[0]
        DAG.__init__(
            self,
            f'{PMIDAG.PRODUCT_DOMAIN}-{PMIDAG.PRODUCT_NAME}-{pipeline_name}-v{deployment_version_major}-{env_id}',
            description=description,
            default_args=default_args,
            schedule_interval=schedule,
            user_defined_filters={
                'data_date_tz_offset': _data_date_tz_offset_filter
            }
        )


    @property
    def pipeline_name(self) -> str:
        """Pipeline name."""
        return self._pipeline_name

    @property
    def app_name(self) -> str:
        """Project's AWS app (stack) name."""
        return self._app_name

    @property
    def env_config(self) -> Mapping[str, Any]:
        """Deployment environment configuration."""
        return self._env_config

    @property
    def env_id(self) -> str:
        """Deployment environment id (shortcut for `env_config['env_id']`)."""
        return self._env_config['env_id']

    @property
    def region_designator(self) -> str:
        """Short deployment region designator ("ue1", "uw2", etc.)."""
        return self._region_designator

    @property
    def aws_conn_id(self) -> str:
        """Airflow AWS connection id (shotcut for `env_config['airflow_aws_conn_id']`)."""
        return self._env_config['airflow_aws_conn_id']

    @property
    def aws_env(self) -> Mapping[str, Any]:
        """AWS environment information.

        See https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-identity-documents.html for details.

        """
        return self._aws_env

    @property
    def assets_location(self) -> str:
        """DAG assets location (an S3 URL)."""
        return self._assets_location
