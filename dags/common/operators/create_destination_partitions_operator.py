"""Glue table partitions support module."""

from airflow.operators.python_operator import PythonOperator
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

from ..pmi_dag import PMIDAG


def _create_destination_paritions(
        *,
        aws_conn_id: str,
        database_name: str,
        table_name: str,
        by_hour: bool,
        ds_nodash: str,
        task: BaseOperator,
        **_
) -> str:
    """Python callable for the `CreateDestinationPartitionsOperator`."""

    aws = AwsHook(aws_conn_id)
    glue = aws.get_client_type('glue')
    log = task.log

    log.info("getting Glue table information for %s.%s", database_name, table_name)
    storage_desc = glue.get_table(
        DatabaseName=database_name,
        Name=table_name
    )['Table']['StorageDescriptor']
    table_location = storage_desc['Location']

    log.info("getting existing partitions")
    existing_partitions = [
        part['Values'] for part in glue.get_partitions(
            DatabaseName=database_name,
            TableName=table_name,
            Expression=f"estdate = '{ds_nodash}'"
        )['Partitions']
    ]
    log.info("found existing partitions: %s", existing_partitions)

    num_parts_created = 0
    if by_hour:
        existing_datehours = set(
            f'{val[0]}:{val[1]}' for val in existing_partitions
        )
        for hour in range(24):
            hour_str = str(hour).zfill(2)
            if f'{ds_nodash}:{hour_str}' not in existing_datehours:
                log.info("creating partition date=%s,hour=%s", ds_nodash, hour_str)
                storage_desc['Location'] = f'{table_location}date={ds_nodash}/hour={hour_str}'
                try:
                    glue.create_partition(
                        DatabaseName=database_name,
                        TableName=table_name,
                        PartitionInput={
                            'StorageDescriptor': storage_desc,
                            'Values': [ds_nodash, hour_str]
                        }
                    )
                    num_parts_created += 1
                except glue.exceptions.AlreadyExistsException:
                    log.info("partition already exists, skipping")
    else:
        if existing_partitions:
            log.info("partition already exists, skipping")
        else:
            log.info("creating partition date=%s", ds_nodash)
            storage_desc['Location'] = f'{table_location}date={ds_nodash}'
            try:
                glue.create_partition(
                    DatabaseName=database_name,
                    TableName=table_name,
                    PartitionInput={
                        'StorageDescriptor': storage_desc,
                        'Values': [ds_nodash]
                    }
                )
                num_parts_created += 1
            except glue.exceptions.AlreadyExistsException:
                log.info("partition already exists, skipping")

    return f"created {num_parts_created} new partitions"


class CreateDestinationPartitionsOperator(PythonOperator):
    """Operator used to create partitions in the target Glue table.

    Parameters
    ----------
    database_name
        Target table's Glue database name.
    partner
        The partner. Used as the target Glue table name.
    by_hour
        If `True`, partitions are created for the data date and all the 24
        individual hours in it. If `False`, only a single partition for the data
        date is created.
    site_data
        If `True`, the data grouped by site table is partitioned. If `False` or
        omitted, regular data table is partitioned.
    args, kwargs
        Standard Airflow operator arguments.

    """

    @apply_defaults
    def __init__(
            self,
            *args,
            database_name: str,
            partner: str,
            by_hour: bool,
            site_data: bool = False,
            **kwargs
    ):
        super().__init__(
            python_callable=_create_destination_paritions,
            provide_context=True,
            op_kwargs={
                'aws_conn_id': PMIDAG.get_dag(**kwargs).aws_conn_id,
                'database_name': database_name,
                'table_name': f'{partner}_site' if site_data else partner,
                'by_hour': by_hour
            },
            *args, **kwargs
        )
