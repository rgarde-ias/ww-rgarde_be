"""Raw partner events Glue table module."""

from aws_cdk import (
    aws_glue as glue,
    aws_s3 as s3,
    core
)

from ias_pmi_cdk_common import PMIApp

from .constants import SHARED_RESOURCES_STACK_NAME_BASE


class JasGlueTable(glue.Table):
    """Partner JAS Glue table."""

    def __init__(
            self, scope: core.Construct, app: PMIApp, cid: str,
            *,
            partner: str,
            bucket: s3.IBucket,
            database: glue.IDatabase
    ):
        super().__init__(
            scope, cid,
            database=database,
            table_name=partner,
            description=f"Ad sessions (JAS) for {partner}.",
            columns=[
                glue.Column(name='impression_id', type=glue.Schema.STRING),
                glue.Column(name='site', type=glue.Schema.STRING),
                glue.Column(name='measurement_source_id', type=glue.Schema.INTEGER),
                glue.Column(name='partner_measured_advertiser_id', type=glue.Schema.BIG_INT),
                glue.Column(name='partner_measured_campaign_id', type=glue.Schema.BIG_INT),
                glue.Column(name='partner_measured_channel_id', type=glue.Schema.BIG_INT),
                glue.Column(name='partner_measured_placement_id', type=glue.Schema.BIG_INT),
                glue.Column(name='partner_measured_creative_id', type=glue.Schema.BIG_INT),
                glue.Column(name='media_type_id', type=glue.Schema.INTEGER),
                glue.Column(name='below_the_fold', type=glue.Schema.BOOLEAN),
                glue.Column(name='on_the_fold', type=glue.Schema.BOOLEAN),
                glue.Column(name='above_the_fold', type=glue.Schema.BOOLEAN),
                glue.Column(name='time_on_page', type=glue.Schema.INTEGER),
                glue.Column(name='in_view_time', type=glue.Schema.INTEGER),
                glue.Column(name='in_view', type=glue.Schema.BOOLEAN),
                glue.Column(name='in_view_5s', type=glue.Schema.BOOLEAN),
                glue.Column(name='in_view_15s', type=glue.Schema.BOOLEAN),
                glue.Column(name='not_in_view', type=glue.Schema.BOOLEAN),
                glue.Column(name='never_in_view', type=glue.Schema.BOOLEAN),
                glue.Column(name='in_view_load', type=glue.Schema.BOOLEAN),
                glue.Column(name='in_view_unload', type=glue.Schema.BOOLEAN),
                glue.Column(name='completed_1q', type=glue.Schema.BOOLEAN),
                glue.Column(name='in_view_1q', type=glue.Schema.BOOLEAN),
                glue.Column(name='completed_2q', type=glue.Schema.BOOLEAN),
                glue.Column(name='in_view_2q', type=glue.Schema.BOOLEAN),
                glue.Column(name='completed_3q', type=glue.Schema.BOOLEAN),
                glue.Column(name='in_view_3q', type=glue.Schema.BOOLEAN),
                glue.Column(name='completed_4q', type=glue.Schema.BOOLEAN),
                glue.Column(name='in_view_4q', type=glue.Schema.BOOLEAN),
                glue.Column(name='never_started', type=glue.Schema.BOOLEAN),
                glue.Column(name='muted', type=glue.Schema.BOOLEAN),
                glue.Column(name='full_screen', type=glue.Schema.BOOLEAN),
                glue.Column(name='click_through', type=glue.Schema.BOOLEAN),
                glue.Column(name='sivt_in_view', type=glue.Schema.BOOLEAN),
                glue.Column(name='sivt_not_in_view', type=glue.Schema.BOOLEAN),
                glue.Column(name='groupm_time_on_page', type=glue.Schema.INTEGER),
                glue.Column(name='groupm_in_view_time', type=glue.Schema.INTEGER),
                glue.Column(name='groupm_in_view', type=glue.Schema.BOOLEAN),
                glue.Column(name='groupm_in_view_5s', type=glue.Schema.BOOLEAN),
                glue.Column(name='groupm_in_view_15s', type=glue.Schema.BOOLEAN),
                glue.Column(name='groupm_not_in_view', type=glue.Schema.BOOLEAN),
                glue.Column(name='groupm_never_in_view', type=glue.Schema.BOOLEAN),
                glue.Column(name='groupm_in_view_load', type=glue.Schema.BOOLEAN),
                glue.Column(name='groupm_in_view_unload', type=glue.Schema.BOOLEAN),
                glue.Column(name='groupm_completed_1q', type=glue.Schema.BOOLEAN),
                glue.Column(name='groupm_in_view_1q', type=glue.Schema.BOOLEAN),
                glue.Column(name='groupm_completed_2q', type=glue.Schema.BOOLEAN),
                glue.Column(name='groupm_in_view_2q', type=glue.Schema.BOOLEAN),
                glue.Column(name='groupm_completed_3q', type=glue.Schema.BOOLEAN),
                glue.Column(name='groupm_in_view_3q', type=glue.Schema.BOOLEAN),
                glue.Column(name='groupm_completed_4q', type=glue.Schema.BOOLEAN),
                glue.Column(name='groupm_in_view_4q', type=glue.Schema.BOOLEAN),
                glue.Column(name='groupm_never_started', type=glue.Schema.BOOLEAN),
                glue.Column(name='groupm_muted', type=glue.Schema.BOOLEAN),
                glue.Column(name='groupm_full_screen', type=glue.Schema.BOOLEAN),
                glue.Column(name='groupm_click_through', type=glue.Schema.BOOLEAN),
                glue.Column(name='groupm_sivt_in_view', type=glue.Schema.BOOLEAN),
                glue.Column(name='groupm_sivt_not_in_view', type=glue.Schema.BOOLEAN),
                glue.Column(name='suspicious', type=glue.Schema.BOOLEAN),
                glue.Column(name='measured', type=glue.Schema.BOOLEAN),
                glue.Column(name='groupm_measured', type=glue.Schema.BOOLEAN),
                glue.Column(name='general_invalid', type=glue.Schema.BOOLEAN),
                glue.Column(name='viewability_measurement_trusted', type=glue.Schema.BOOLEAN),
                glue.Column(name='sitting_duck_bot', type=glue.Schema.BOOLEAN),
                glue.Column(name='standard_bot', type=glue.Schema.BOOLEAN),
                glue.Column(name='volunteer_bot', type=glue.Schema.BOOLEAN),
                glue.Column(name='profile_bot', type=glue.Schema.BOOLEAN),
                glue.Column(name='masked_bot', type=glue.Schema.BOOLEAN),
                glue.Column(name='nomadic_bot', type=glue.Schema.BOOLEAN),
                glue.Column(name='other_bot', type=glue.Schema.BOOLEAN),
                glue.Column(name='true_view_viewable', type=glue.Schema.BOOLEAN),
                glue.Column(name='true_view_measurable', type=glue.Schema.BOOLEAN),
                glue.Column(name='yahoo_gemini_billable', type=glue.Schema.BOOLEAN),
                glue.Column(name='full_ad_in_view', type=glue.Schema.BOOLEAN),
                glue.Column(name='publicis_in_view', type=glue.Schema.BOOLEAN),
                glue.Column(name='yahoo_gemini_billable_suspicious', type=glue.Schema.BOOLEAN),
                glue.Column(name='average_in_view_time', type=glue.Schema.DOUBLE),
                glue.Column(name='in_view_lt_1s', type=glue.Schema.BOOLEAN),
                glue.Column(name='in_view_1s_2s', type=glue.Schema.BOOLEAN),
                glue.Column(name='in_view_2s_5s', type=glue.Schema.BOOLEAN),
                glue.Column(name='in_view_5s_10s', type=glue.Schema.BOOLEAN),
                glue.Column(name='in_view_10s_15s', type=glue.Schema.BOOLEAN),
                glue.Column(name='in_view_15s_20s', type=glue.Schema.BOOLEAN),
                glue.Column(name='in_view_20s_25s', type=glue.Schema.BOOLEAN),
                glue.Column(name='in_view_25s_30s', type=glue.Schema.BOOLEAN),
                glue.Column(name='in_view_30s_35s', type=glue.Schema.BOOLEAN),
                glue.Column(name='in_view_35s_40s', type=glue.Schema.BOOLEAN),
                glue.Column(name='in_view_40s_45s', type=glue.Schema.BOOLEAN),
                glue.Column(name='in_view_45s_50s', type=glue.Schema.BOOLEAN),
                glue.Column(name='in_view_ge_50s', type=glue.Schema.BOOLEAN),
                glue.Column(name='viewability_measured_or_fraud', type=glue.Schema.BOOLEAN)
            ],
            partition_keys=[
                glue.Column(name='estdate', type=glue.Schema.STRING),
                glue.Column(name='esthour', type=glue.Schema.STRING)
            ],
            data_format=glue.DataFormat.AVRO,
            bucket=bucket,
            s3_prefix=core.Fn.join('', [
                core.Fn.import_value(f'{SHARED_RESOURCES_STACK_NAME_BASE}-{app.env_id}:DataLakeJASBucketDataPrefix'),
                partner,
                '/'
            ])
        )

        # data retention period
        cfn_table: glue.CfnTable = self.node.default_child
        cfn_table.add_property_override('TableInput.Retention', 90)
