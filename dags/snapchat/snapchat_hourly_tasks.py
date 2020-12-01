"""Snapchat hourly tasks module."""

from ..common import PMIDAG
from ..common.operators import UpdateFirewallNameMappingsOperator, LoadNewFilesOperator

from .constants import PARTNER, MSOURCE_ID


SFTP_MAPPINGS_DATA_PREFIX = 'sftp/SnapchatSFTP/inbound'
SFTP_DISCREPANCIES_DATA_PREFIX = 'sftp/SnapchatSFTP/inbound'

TIMESTAMP_PATTERN = r'_(\d{8})(.*).csv$'


class SnapchatHourlyTasks:
    """Snapchat hourly tasks."""
    # pylint: disable=too-few-public-methods
    def __init__(self):

        dag = PMIDAG.get_dag()
        sftp_bucket_name = dag.env_config['sftp_bucket_name']

        UpdateFirewallNameMappingsOperator(
            task_id=f'{PARTNER}_update_advertisers',
            mappings_bucket_name=sftp_bucket_name,
            mappings_prefix=f'{SFTP_MAPPINGS_DATA_PREFIX}/advertiser_generic_',
            mappings_timestamp_pattern=TIMESTAMP_PATTERN,
            firewall_table='PARTNER_MEASURED_ADVERTISER',
            msource_id=MSOURCE_ID
        )

        UpdateFirewallNameMappingsOperator(
            task_id=f'{PARTNER}_update_campaigns',
            mappings_bucket_name=sftp_bucket_name,
            mappings_prefix=f'{SFTP_MAPPINGS_DATA_PREFIX}/campaign_generic_',
            mappings_timestamp_pattern=TIMESTAMP_PATTERN,
            firewall_table='PARTNER_MEASURED_CAMPAIGN',
            msource_id=MSOURCE_ID
        )

        UpdateFirewallNameMappingsOperator(
            task_id=f'{PARTNER}_update_creatives',
            mappings_bucket_name=sftp_bucket_name,
            mappings_prefix=f'{SFTP_MAPPINGS_DATA_PREFIX}/creative_generic_',
            mappings_timestamp_pattern=TIMESTAMP_PATTERN,
            firewall_table='PARTNER_MEASURED_CREATIVE',
            msource_id=MSOURCE_ID
        )

        UpdateFirewallNameMappingsOperator(
            task_id=f'{PARTNER}_update_placements',
            mappings_bucket_name=sftp_bucket_name,
            mappings_prefix=f'{SFTP_MAPPINGS_DATA_PREFIX}/placement_generic_',
            mappings_timestamp_pattern=TIMESTAMP_PATTERN,
            firewall_table='PARTNER_MEASURED_PLACEMENT',
            msource_id=MSOURCE_ID
        )

        LoadNewFilesOperator(
            task_id=f'{PARTNER}_load_discrepancies',
            bucket_name=sftp_bucket_name,
            prefix=f'{SFTP_DISCREPANCIES_DATA_PREFIX}/discrepency_counts_',
            files_type_id=f'discrepancies.{PARTNER}'
        )
