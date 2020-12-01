"""Snapchat campaign id mappings pipeline implementation module."""
from typing import Sequence, Mapping, Tuple, Set
from logging import Logger
import json
from contextlib import closing

from MySQLdb.cursors import Cursor

from airflow.models.baseoperator import BaseOperator, TaskInstance
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.aws_sns_hook import AwsSnsHook
from airflow.utils.helpers import chain

from ..common import BasePartnerPipeline, PMIDAG, EmrCluster
from ..common.operators import ClearDestinationBucketOperator

from .constants import PARTNER, MSOURCE_ID


def _load_extracted_mappings(
        *,
        aws_conn_id: str,
        bucket_name: str,
        bucket_data_prefix: str,
        partner: str,
        ds_nodash: str,
        log: Logger
) -> Sequence[Mapping[str, int]]:
    """Load extracted mappings from S3."""

    full_prefix = f'{bucket_data_prefix}{partner}/date={ds_nodash}/'
    log.info("loading extracted mappings from s3://%s/%s", bucket_name, full_prefix)
    aws = AwsHook(aws_conn_id)
    bucket = aws.get_resource_type('s3').Bucket(bucket_name)
    extracted_mappings = [
        json.loads(line) for file_obj in bucket.objects.filter(
            Prefix=full_prefix
        ) for line in file_obj.get()['Body'].iter_lines()
    ]
    log.info("loaded mappings: %s", extracted_mappings)

    return extracted_mappings


def _update_firewall_db(
        *,
        cur: Cursor,
        log: Logger,
        extracted_mappings: Sequence[Mapping[str, int]]
) -> Tuple[Set[int], Set[Tuple[int, int, int]]]:
    """Update firewall DB with the extracted campaign id mappings."""

    # map adv entity ids from events to IAS campaign ids
    ias_adv_entity_ids_sql = ', '.join(str(v) for v in set(
        mapping['ias_adv_entity_id'] for mapping in extracted_mappings
    ))
    query = (
        f"SELECT ID, CAMPAIGN_ID"
        f" FROM ADV_ENTITY"
        f" WHERE ID IN ({ias_adv_entity_ids_sql})"
        f" LOCK IN SHARE MODE"
    )
    log.info("getting IAS campaign ids for adv entities: %s", query)
    cur.execute(query)
    ias_campaign_ids = {
        row[0]: row[1] for row in cur.fetchall()
    }
    log.info("result (adv entity id/IAS campaign id): %s", ias_campaign_ids)

    # load existing campaign mappings
    partner_campaign_ids_sql = ', '.join(str(v) for v in set(
        mapping['partner_measured_campaign_id'] for mapping in extracted_mappings
    ))
    query = (
        f"SELECT ID, CAMPAIGN_ID"
        f" FROM PARTNER_MEASURED_CAMPAIGN"
        f" WHERE ID IN ({partner_campaign_ids_sql})"
        f" AND MEASUREMENT_SOURCE_ID = {MSOURCE_ID}"
        f" FOR UPDATE"
    )
    log.info("getting existing mappings: %s", query)
    cur.execute(query)
    existing_ias_campaign_ids = {
        row[0]: (row[1] if row[1] is not None else None) for row in cur.fetchall()
    }
    log.info("result (IAS campaign id/partner campaign id): %s", existing_ias_campaign_ids)

    # collect exceptions
    invalid_ias_adv_entity_ids = set()
    overriding_campaign_id_mappings = set()

    # update mappings
    for mapping in extracted_mappings:
        partner_campaign_id = mapping['partner_measured_campaign_id']
        ias_adv_entity_id = mapping['ias_adv_entity_id']
        ias_campaign_id = ias_campaign_ids.get(ias_adv_entity_id)
        existing_ias_campaign_id = existing_ias_campaign_ids.get(partner_campaign_id)
        if ias_campaign_id is None:
            log.warning("invalid adv entity id %s", ias_adv_entity_id)
            invalid_ias_adv_entity_ids.add(ias_adv_entity_id)
        elif (
                existing_ias_campaign_id is not None
                and existing_ias_campaign_id != ias_campaign_id
        ):
            log.warning(
                "partner campaign %s is already linked to a different IAS campaign %s",
                partner_campaign_id, existing_ias_campaign_id
            )
            overriding_campaign_id_mappings.add((
                partner_campaign_id, existing_ias_campaign_id, ias_campaign_id
            ))
        elif existing_ias_campaign_id == ias_campaign_id:
            log.info(
                "partner campaign %s is already linked to IAS campaign %s, skipping it",
                partner_campaign_id, existing_ias_campaign_id
            )
        elif partner_campaign_id in existing_ias_campaign_ids:
            query = (
                f"UPDATE PARTNER_MEASURED_CAMPAIGN"
                f" SET CAMPAIGN_ID = {ias_campaign_id}"
                f" WHERE ID = {partner_campaign_id}"
                f" AND MEASUREMENT_SOURCE_ID = {MSOURCE_ID}"
            )
            log.info("setting campaign mapping: %s", query)
            cur.execute(query)
        else:
            query = (
                f"INSERT INTO PARTNER_MEASURED_CAMPAIGN"
                f" (MEASUREMENT_SOURCE_ID, ID, NAME, CAMPAIGN_ID)"
                f" VALUES ({MSOURCE_ID}, {partner_campaign_id}"
                f", 'DATA NOT RECEIVED', {ias_campaign_id})"
            )
            log.info("creating new mapping: %s", query)
            cur.execute(query)

    # return exceptions
    return (invalid_ias_adv_entity_ids, overriding_campaign_id_mappings)


def _save_campaign_id_mappings(
        *,
        aws_conn_id: str,
        mysql_conn_id: str,
        bucket_name: str,
        bucket_data_prefix: str,
        partner: str,
        notifications_topic_arn: str,
        ds_nodash: str,
        task: BaseOperator,
        task_instance: TaskInstance,
        **_
):
    """Python callable for the operator that saves campaign id mappings in the firewall DB."""

    log = task.log

    # read new mappings from S3
    extracted_mappings = _load_extracted_mappings(
        aws_conn_id=aws_conn_id,
        bucket_name=bucket_name,
        bucket_data_prefix=bucket_data_prefix,
        partner=partner,
        ds_nodash=ds_nodash,
        log=log

    )
    if not extracted_mappings:
        return "no mappings have been extracted, bailing out"

    # connect to firewall database and update it
    log.info("connecting to firewall database")
    mysql = MySqlHook(mysql_conn_id=mysql_conn_id)
    with closing(mysql.get_conn()) as conn:
        mysql.set_autocommit(conn, False)
        with closing(conn.cursor()) as cur:
            (invalid_ias_adv_entity_ids, overriding_campaign_id_mappings) = _update_firewall_db(
                cur=cur,
                log=log,
                extracted_mappings=extracted_mappings
            )
        log.info("committing transaction")
        conn.commit()

    # send notification about any exceptions
    if invalid_ias_adv_entity_ids or overriding_campaign_id_mappings:
        log.info("sending mapping exceptions notification")
        invalid_ias_adv_entity_ids_msg = ', '.join(
            str(eid) for eid in invalid_ias_adv_entity_ids
        ) if invalid_ias_adv_entity_ids else 'None'
        overriding_campaign_id_mappings_msg = '\n'.join(
            '{:<19} | {:<24} | {:<19}'.format(*row) for row in overriding_campaign_id_mappings
        ) if overriding_campaign_id_mappings else 'None'
        sns = AwsSnsHook(aws_conn_id=aws_conn_id)
        sns.publish_to_target(
            target_arn=notifications_topic_arn,
            subject=f'Campaign ID mapping exceptions ({partner})',
            message=(
                f"Encountered campaign ID mapping exceptions:\n"
                f"\n"
                f"\nDAG:             {task_instance.dag_id}"
                f"\nTask:            {task_instance.task_id}"
                f"\nExecution Date:  {task_instance.execution_date}"
                f"\nHost:            {task_instance.hostname}"
                f"\n"
                f"\nUnknown IAS adv entity IDs:"
                f"\n"
                f"\n{invalid_ias_adv_entity_ids_msg}"
                f"\n"
                f"\nAttempts to change existing mappings:"
                f"\n"
                f"\npartner campaign ID | existing IAS campaign ID | new IAS campaign ID"
                f"\n{overriding_campaign_id_mappings_msg}"
            )
        )

    # done
    return "campaign id mappings have been updated"


class SnapchatCampaignIdMappingsPipeline(BasePartnerPipeline):

    """Snapchat campaign id mappings pipeline implementation.

    Parameters
    ----------
    emr_cluster
        The EMR cluster.

    """

    def __init__(
            self,
            *,
            emr_cluster: EmrCluster
    ):
        super().__init__(PARTNER)

        dag = PMIDAG.get_dag()
        env_config = dag.env_config

        campaign_id_mappings_bucket = env_config['itermediate_bucket_name']
        campaign_id_mappings_prefix = 'campaigns/'

        # erase existing campaign id mappings data
        erase_campaign_id_mappings_task = ClearDestinationBucketOperator(
            task_id=f'{self.partner}_erase_campaign_id_mappings',
            bucket_name=campaign_id_mappings_bucket,
            bucket_data_prefix=campaign_id_mappings_prefix,
            partner=self.partner
        )

        # perform campaign id mappings extraction
        campaign_id_mappings_extraction_task = emr_cluster.create_emr_job_task(
            task_id=f'{self.partner}_campaign_id_mappings_extraction',
            job_name=f'Campaign ID Mappings Extraction: {self.partner}',
            script='campaign_id_mappings.py',
            script_args=[
                '--partner', self.partner,
                '--data-date', '{{ ds }}',
                '--data-date-tz-offset', '{{ ds | data_date_tz_offset }}',
                '--input-database-name', env_config['raw_events_glue_db_name'],
                '--output-bucket-name', campaign_id_mappings_bucket,
                '--output-bucket-data-prefix', campaign_id_mappings_prefix
            ]
        )

        # save extracted mappings in firewall DB
        save_campaign_id_mappings_task = PythonOperator(
            task_id=f'{self.partner}_save_campaign_id_mappings',
            python_callable=_save_campaign_id_mappings,
            provide_context=True,
            op_kwargs={
                'aws_conn_id': dag.aws_conn_id,
                'mysql_conn_id': env_config['airflow_firewall_db_conn_id'],
                'bucket_name': campaign_id_mappings_bucket,
                'bucket_data_prefix': campaign_id_mappings_prefix,
                'partner': self.partner,
                'notifications_topic_arn': ':'.join([
                    "arn:aws:sns",
                    dag.aws_env['region'],
                    dag.aws_env['accountId'],
                    env_config['notifications_topic_name']
                ])
            }
        )

        # connect tasks
        chain(
            erase_campaign_id_mappings_task,
            campaign_id_mappings_extraction_task,
            save_campaign_id_mappings_task
        )

        # set pipeline entry and exit
        self._entry_task = erase_campaign_id_mappings_task
        self._exit_task = save_campaign_id_mappings_task


    @property
    def entry_task(self):
        return self._entry_task

    @property
    def exit_task(self):
        return self._exit_task
