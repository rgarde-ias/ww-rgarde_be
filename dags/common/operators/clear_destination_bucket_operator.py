"""Datalake S3 buckets support module."""

from airflow.operators.python_operator import PythonOperator
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.exceptions import AirflowException

from ..pmi_dag import PMIDAG


def _clear_destination_bucket(
        *,
        aws_conn_id: str,
        bucket_name: str,
        bucket_data_prefix: str,
        table_name: str,
        ds_nodash: str,
        task: BaseOperator,
        **_
) -> str:
    """Python callable for the `ClearDestinationBucketOperator`."""

    log = task.log

    aws = AwsHook(aws_conn_id)
    s3 = aws.get_resource_type('s3') # pylint: disable=invalid-name

    prefix = f'{bucket_data_prefix}{table_name}/date={ds_nodash}/'

    log.info("erasing any existing data in s3://%s/%s", bucket_name, prefix)
    resp = s3.Bucket(bucket_name).objects.filter(Prefix=prefix).delete()
    log.info("got response: %s", resp)

    if list(item for single_resp in resp for item in single_resp.get('Errors', [])):
        raise AirflowException(f"Unable to fully erase existing data in s3://{bucket_name}/{prefix}")

    return f"erased {len(list(item for single_resp in resp for item in single_resp.get('Deleted', [])))} files"


class ClearDestinationBucketOperator(PythonOperator):
    """Operator used to erase any existing data in the target datalake S3 bucket
    before writing new data to it.

    The operator erases all existing data for the whole data date.

    Parameters
    ----------
    bucket_name
        Target S3 bucket name.
    bucket_data_prefix
        S3 key prefix (folder) in the target S3 bucket.
    partner
        The partner. Used as the target Glue table name.
    site_data
        If `True`, the data grouped by site is cleared. If `False` or omitted,
        regular data is cleared.
    args, kwargs
        Standard Airflow operator arguments.

    """

    @apply_defaults
    def __init__(
            self,
            *args,
            bucket_name: str,
            bucket_data_prefix: str,
            partner: str,
            site_data: bool = False,
            **kwargs
    ):
        super().__init__(
            python_callable=_clear_destination_bucket,
            provide_context=True,
            op_kwargs={
                'aws_conn_id': PMIDAG.get_dag(**kwargs).aws_conn_id,
                'bucket_name': bucket_name,
                'bucket_data_prefix': bucket_data_prefix,
                'table_name': f'{partner}_site' if site_data else partner
            },
            retries=1,
            *args, **kwargs
        )
