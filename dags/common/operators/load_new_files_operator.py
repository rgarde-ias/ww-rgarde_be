"""Load new files operator module."""

import json

from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_sns_hook import AwsSnsHook
from airflow.exceptions import AirflowSkipException
from airflow.models.baseoperator import BaseOperator


from ..pmi_dag import PMIDAG
from ..hooks import PipelineStateHook


def _file_state_key(files_type_id: str, file_s3_key: str) -> str:
    """Get pipeline state record key for the specified file S3 key.

    Parameters
    ----------
    files_type_id
        Files type id.
    file_s3_key
        File's S3 key.

    Returns
    -------
    str
        Corresponding pipeline state record key.

    """

    file_name = file_s3_key[file_s3_key.rfind('/') + 1:]

    return f'{files_type_id}.{file_name}.processed_etag'


def _load_new_files(
        *,
        aws_conn_id: str,
        state_table_name: str,
        bucket_name: str,
        prefix: str,
        files_type_id: str,
        data_load_trigger_sns_topic_arn: str,
        data_load_trigger_client_id: str,
        task: BaseOperator,
        **_
):
    """Python callable for the `LoadNewFilesOperator`."""

    log = task.log

    pipeline_state = PipelineStateHook(state_table_name, aws_conn_id=aws_conn_id)
    bucket = S3Hook(aws_conn_id).get_bucket(bucket_name)
    sns = AwsSnsHook(aws_conn_id)

    # list files in the bucket and find new ones
    new_files = []
    for file_obj in bucket.objects.filter(Prefix=prefix):
        state_key = _file_state_key(files_type_id, file_obj.key)
        state_value = pipeline_state.get_state(state_key)
        if state_value is None or state_value != file_obj.e_tag:
            new_files.append(file_obj)
    log.info("new files: %s", new_files)

    # check if found any new files
    if not new_files:
        raise AirflowSkipException("no new files found")

    # process each new file
    for file_obj in new_files:

        # trigger data loader
        file_s3_url = f's3://{bucket.name}/{file_obj.key}'
        log.info("triggering data load for %s", file_s3_url)
        sns.publish_to_target(
            target_arn=data_load_trigger_sns_topic_arn,
            message=json.dumps({
                'client_id': data_load_trigger_client_id,
                'data_url': file_s3_url
            })
        )

        # save state
        state_key = _file_state_key(files_type_id, file_obj.key)
        pipeline_state.save_state(state_key, file_obj.e_tag)


class LoadNewFilesOperator(PythonOperator):
    """Operator used to trigger data loader for new files in the S3 location.

    The operator lists files matching certain prefix in the specified S3
    location and if it finds any new files that it hasn't processed before, it
    triggers the data loader for the files.

    Parameters
    ----------
    bucket_name
        Name of the S3 bucket where the files are expected.
    prefix
        S3 key prefix for the matching files.
    files_type_id
        Identifier for the files type used to store the previously loaded files
        state.

    """

    def __init__(
            self,
            *args,
            bucket_name: str,
            prefix: str,
            files_type_id: str,
            **kwargs
    ):
        dag = PMIDAG.get_dag(**kwargs)
        super().__init__(
            python_callable=_load_new_files,
            provide_context=True,
            op_kwargs={
                'aws_conn_id': dag.aws_conn_id,
                'state_table_name': dag.env_config['pipeline_state_table_name'],
                'bucket_name': bucket_name,
                'prefix': prefix,
                'files_type_id': files_type_id,
                'data_load_trigger_sns_topic_arn': dag.env_config['data_load_trigger_sns_topic_arn'],
                'data_load_trigger_client_id': dag.app_name
            },
            *args, **kwargs
        )
