"""Firewall database updates support module."""
import json
from typing import Sequence, Tuple
import re
from logging import Logger
from contextlib import closing
import csv

from MySQLdb.cursors import Cursor

from airflow.operators.python_operator import PythonOperator
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.exceptions import AirflowSkipException

from ..pmi_dag import PMIDAG
from ..hooks import PipelineStateHook


def _sql_str_lit(val: str) -> str:
    """Transform string to SQL string literal."""
    return "'" + val.replace("'", "''") + "'"


def _flush_mappings_batch(
        cur: Cursor,
        firewall_table: str,
        msource_id: int,
        mappings: Sequence[Tuple[int, str]]
) -> None:
    """Flush accumulated mappings batch to Firewall database."""

    query = (
        f"INSERT INTO {firewall_table}"
        f" (MEASUREMENT_SOURCE_ID, ID, NAME, CREATED_BY, CREATED_ON)"
        f" VALUES"
        ) + ','.join(
            " (" + ', '.join([
                str(msource_id),
                str(mapping[0]),
                _sql_str_lit(mapping[1]),
                str(-2),
                'CURRENT_TIMESTAMP()'
            ]) + ")" for mapping in mappings
        ) + (
            " ON DUPLICATE KEY UPDATE"
            " NAME = VALUES(NAME), MODIFIED_BY = -2, LAST_MODIFIED = CURRENT_TIMESTAMP()"
            )

    cur.execute(query)


def _load_and_process_mappings(
        *,
        cur: Cursor,
        mappings_file,
        firewall_table: str,
        msource_id: int,
        log: Logger
) -> None:
    """Process mappings file and update the firewall table."""

    log.info(
        "loading mappings from s3://%s/%s",
        mappings_file.bucket_name, mappings_file.key
    )
    mapping_file_lines = mappings_file.get()['Body'].iter_lines()
    next(mapping_file_lines)  # skip the header

    last_row = None
    mappings = []
    for row in csv.reader(line.decode('utf-8') for line in mapping_file_lines):
        if row != last_row:  # primitive dedupe
            mappings.append((int(row[0]), row[1]))
            if len(mappings) >= 1000:
                log.info("flushing %s mappings to the database", len(mappings))
                _flush_mappings_batch(cur, firewall_table, msource_id, mappings)
                mappings.clear()
        last_row = row

    if mappings:
        log.info("flushing %s mappings to the database", len(mappings))
        _flush_mappings_batch(cur, firewall_table, msource_id, mappings)


def _is_legacy_state_value(state_value) -> bool:
    return '|' in state_value


def _get_latest_processed_ts(state_value: str) -> str:
    if _is_legacy_state_value(state_value):
        return state_value.split('|')[0]

    value = json.loads(state_value)
    return value['timestamp']


def _get_latest_processed_etags(state_value: str) -> set:
    etags = set()
    if _is_legacy_state_value(state_value):
        etags.add(state_value.split('|')[1])
    else:
        value = json.loads(state_value)
        for partition in value['partitions']:
            etags.add(partition['etag'])
    return etags


def _get_latest_processed_partitions(state_value: str) -> list:
    if _is_legacy_state_value(state_value):
        return [{
            'filename': '',
            'etag': state_value.split('|')[1]
        }]

    value = json.loads(state_value)
    return value['partitions']


def _get_filename(s3_key: str) -> str:
    return s3_key.split('/')[-1]


def _create_state(old_state_value: str, latest_available_ts: str, latest_available_mapping_files: list) -> dict:
    partitions = []
    for mapping_file in latest_available_mapping_files:
        partitions.append({
            'filename': _get_filename(mapping_file.key),
            'etag': mapping_file.e_tag
        })
    if not old_state_value:
        return {
            'timestamp': latest_available_ts,
            'partitions': partitions
        }
    if _get_latest_processed_ts(old_state_value) == latest_available_ts:
        processed_partitions = _get_latest_processed_partitions(old_state_value)
        partitions.extend(processed_partitions)

    return {
        'timestamp': latest_available_ts,
        'partitions': partitions
    }


def _update_firewall_name_mappings(
        *,
        aws_conn_id: str,
        mysql_conn_id: str,
        state_table_name: str,
        mappings_bucket_name: str,
        mappings_prefix: str,
        mappings_timestamp_pattern: str,
        firewall_table: str,
        msource_id: int,
        task: BaseOperator,
        **_
) -> str:
    """Python callable for the `UpdateFirewallNameMappingsOperator`."""
    # pylint: disable=too-many-locals

    log = task.log

    pipeline_state = PipelineStateHook(state_table_name, aws_conn_id=aws_conn_id)
    mappings_bucket = S3Hook(aws_conn_id).get_bucket(mappings_bucket_name)

    # get latest processed mappings file timestamp
    state_key = f'name_mappings.{firewall_table}.{msource_id}.latest_processed'
    state_value = pipeline_state.get_state(state_key)
    if state_value is not None:
        latest_processed_ts = _get_latest_processed_ts(state_value)
        latest_processed_etags = _get_latest_processed_etags(state_value)
    else:
        latest_processed_ts = '0'
        latest_processed_etags = set()

    # list files in the bucket and find the newest one
    mappings_timestamp_re = re.compile(mappings_timestamp_pattern)
    latest_available_ts = '0'
    latest_available_files = dict()
    for mappings_file in mappings_bucket.objects.filter(Prefix=mappings_prefix):
        match = mappings_timestamp_re.search(mappings_file.key)
        if match is not None:
            available_ts = match.group(1)
            if available_ts > latest_available_ts:
                latest_available_ts = available_ts
                latest_available_files = {mappings_file.e_tag: mappings_file}
            elif latest_available_ts == available_ts:
                latest_available_files.update({mappings_file.e_tag: mappings_file})

    # If the files for the same date, we must skip processed files
    if latest_available_ts == latest_processed_ts:
        for latest_processed_etag in latest_processed_etags:
            log.info("skipping the file=%s, etag=%s",
                     _get_filename(latest_available_files[latest_processed_etag].key),
                     latest_available_files[latest_processed_etag].e_tag)
            del latest_available_files[latest_processed_etag]

    # check if no newer file
    if (not latest_available_files
            or latest_available_ts < latest_processed_ts
            or (
                latest_available_ts == latest_processed_ts
                and not latest_available_files
                )
       ):
        raise AirflowSkipException("no newer mappings file found")

    # connect to the firewall database and process the mappings
    log.info("connecting to firewall database")
    mysql = MySqlHook(mysql_conn_id=mysql_conn_id)
    with closing(mysql.get_conn()) as conn:
        mysql.set_autocommit(conn, False)
        for latest_available_file in latest_available_files.values():
            log.info("processing filename=%s, etag=%s, ts=%s",
                     _get_filename(latest_available_file.key),
                     latest_available_file.e_tag,
                     latest_available_ts)
            with closing(conn.cursor()) as cur:
                _load_and_process_mappings(
                    cur=cur,
                    mappings_file=latest_available_file,
                    firewall_table=firewall_table,
                    msource_id=msource_id,
                    log=log
                )
            log.info("committing transaction")
            conn.commit()

    # update state table
    new_state_value = _create_state(state_value, latest_available_ts, latest_available_files.values())
    log.info('saving new state: key=%s, value=%s', state_key, new_state_value)
    pipeline_state.save_state(state_key, json.dumps(new_state_value))

    # done
    return "new mappings have been processed"


class UpdateFirewallNameMappingsOperator(PythonOperator):
    """Operator used to update Firewall DB ID/name mapping tables.

    The operator loads a CSV file from an S3 location. The CSV file includes two
    columns: one for the ID value and one for the corresponding name. The
    operator then updates an ID/name reference table in the Firewall database
    with the data from the CSV file. The operator also keeps track of the latest
    processed CSV file and updates the reference table only if new files are
    detected. If no nre files found, the task is marked as skipped.

    Parameters
    ----------
    mappings_bucket_name
        Name of the S3 bucket with mapping CSV files.
    mappings_prefix
        S3 key prefix for the mapping CSV files in the mappings bucket.
    mappings_timestamp_pattern
        Regexp pattern for matching mapping CSV files and extracting the file
        timestamp value.
    firewall_table
        Name of the target mappings Firewall database table.
    msource_id
        Partner measurement source ID.

    """

    def __init__(
            self,
            *args,
            mappings_bucket_name: str,
            mappings_prefix: str,
            mappings_timestamp_pattern: str,
            firewall_table: str,
            msource_id: int,
            **kwargs
    ):
        dag = PMIDAG.get_dag(**kwargs)
        super().__init__(
            python_callable=_update_firewall_name_mappings,
            provide_context=True,
            op_kwargs={
                'aws_conn_id': dag.aws_conn_id,
                'mysql_conn_id': dag.env_config['airflow_firewall_db_conn_id'],
                'state_table_name': dag.env_config['pipeline_state_table_name'],
                'mappings_bucket_name': mappings_bucket_name,
                'mappings_prefix': mappings_prefix,
                'mappings_timestamp_pattern': mappings_timestamp_pattern,
                'firewall_table': firewall_table,
                'msource_id': msource_id
            },
            *args, **kwargs
        )
