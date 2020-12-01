"""Data loader support module."""

import json

from airflow.contrib.operators.sns_publish_operator import SnsPublishOperator
from airflow.utils.decorators import apply_defaults

from ..pmi_dag import PMIDAG


class DataLoaderTriggerOperator(SnsPublishOperator):
    """Data loader trigger operator.

    This operator triggers the data load from the JAS mart datalake bucket for
    the specified partner and the DAG run's data date. Therefore, this operator
    is normally used at the end of the daily viewability aggregation pipeline.

    Parameters
    ----------
    partner
        The partner.
    site_data
        If `True`, the viewability data grouped by site is loaded. If `False` or
        omitted, regular viewability data is loaded.
    args, kwargs
        Standard Airflow operator arguments.

    """

    @apply_defaults
    def __init__(
            self,
            *args,
            partner: str,
            site_data: bool = False,
            **kwargs
    ):
        dag = PMIDAG.get_dag(**kwargs)
        table_name = f'{partner}_site' if site_data else partner
        super().__init__(
            aws_conn_id=dag.aws_conn_id,
            target_arn=dag.env_config['data_load_trigger_sns_topic_arn'],
            message=json.dumps({
                'client_id': dag.app_name,
                'data_url': (
                    f"s3://{dag.env_config['jas_mart_bucket_name']}"
                    f"/{dag.env_config['jas_mart_bucket_data_prefix']}"
                    f"{table_name}/date={{{{ ds_nodash }}}}/"
                )
            }),
            *args, **kwargs
        )
