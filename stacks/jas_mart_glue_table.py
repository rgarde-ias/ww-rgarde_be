"""Raw partner events Glue table module."""

from aws_cdk import (
    aws_glue as glue,
    aws_s3 as s3,
    core
)

from ias_pmi_cdk_common import PMIApp

from .constants import SHARED_RESOURCES_STACK_NAME_BASE


class JasMartGlueTable(glue.Table):
    """Partner aggregated viewability metrics Glue table."""

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
            description=f"Aggregated viewability metrics for {partner}.",
            columns=[
                glue.Column(name='hit_date', type=glue.Schema.DATE),
                glue.Column(name='measurement_source_id', type=glue.Schema.INTEGER),
                glue.Column(name='partner_measured_advertiser_id', type=glue.Schema.BIG_INT),
                glue.Column(name='partner_measured_campaign_id', type=glue.Schema.BIG_INT),
                glue.Column(name='partner_measured_channel_id', type=glue.Schema.BIG_INT),
                glue.Column(name='partner_measured_placement_id', type=glue.Schema.BIG_INT),
                glue.Column(name='partner_measured_creative_id', type=glue.Schema.BIG_INT),
                glue.Column(name='media_type_id', type=glue.Schema.INTEGER),
                glue.Column(name='below_the_fold_imps', type=glue.Schema.INTEGER),
                glue.Column(name='on_the_fold_imps', type=glue.Schema.INTEGER),
                glue.Column(name='above_the_fold_imps', type=glue.Schema.INTEGER),
                glue.Column(name='time_on_page', type=glue.Schema.BIG_INT),
                glue.Column(name='in_view_time', type=glue.Schema.BIG_INT),
                glue.Column(name='in_view_imps', type=glue.Schema.INTEGER),
                glue.Column(name='in_view_5s_imps', type=glue.Schema.INTEGER),
                glue.Column(name='in_view_15s_imps', type=glue.Schema.INTEGER),
                glue.Column(name='not_in_view_imps', type=glue.Schema.INTEGER),
                glue.Column(name='never_in_view_imps', type=glue.Schema.INTEGER),
                glue.Column(name='in_view_load_imps', type=glue.Schema.INTEGER),
                glue.Column(name='in_view_unload_imps', type=glue.Schema.INTEGER),
                glue.Column(name='completed_1q_imps', type=glue.Schema.INTEGER),
                glue.Column(name='in_view_1q_imps', type=glue.Schema.INTEGER),
                glue.Column(name='completed_2q_imps', type=glue.Schema.INTEGER),
                glue.Column(name='in_view_2q_imps', type=glue.Schema.INTEGER),
                glue.Column(name='completed_3q_imps', type=glue.Schema.INTEGER),
                glue.Column(name='in_view_3q_imps', type=glue.Schema.INTEGER),
                glue.Column(name='completed_4q_imps', type=glue.Schema.INTEGER),
                glue.Column(name='in_view_4q_imps', type=glue.Schema.INTEGER),
                glue.Column(name='never_started_imps', type=glue.Schema.INTEGER),
                glue.Column(name='muted_imps', type=glue.Schema.INTEGER),
                glue.Column(name='full_screen_imps', type=glue.Schema.INTEGER),
                glue.Column(name='click_through_imps', type=glue.Schema.INTEGER),
                glue.Column(name='sivt_in_view_imps', type=glue.Schema.INTEGER),
                glue.Column(name='sivt_not_in_view_imps', type=glue.Schema.INTEGER),
                glue.Column(name='groupm_time_on_page', type=glue.Schema.BIG_INT),
                glue.Column(name='groupm_in_view_time', type=glue.Schema.BIG_INT),
                glue.Column(name='groupm_in_view_imps', type=glue.Schema.INTEGER),
                glue.Column(name='groupm_in_view_5s_imps', type=glue.Schema.INTEGER),
                glue.Column(name='groupm_in_view_15s_imps', type=glue.Schema.INTEGER),
                glue.Column(name='groupm_not_in_view_imps', type=glue.Schema.INTEGER),
                glue.Column(name='groupm_never_in_view_imps', type=glue.Schema.INTEGER),
                glue.Column(name='groupm_in_view_load_imps', type=glue.Schema.INTEGER),
                glue.Column(name='groupm_in_view_unload_imps', type=glue.Schema.INTEGER),
                glue.Column(name='groupm_completed_1q_imps', type=glue.Schema.INTEGER),
                glue.Column(name='groupm_in_view_1q_imps', type=glue.Schema.INTEGER),
                glue.Column(name='groupm_completed_2q_imps', type=glue.Schema.INTEGER),
                glue.Column(name='groupm_in_view_2q_imps', type=glue.Schema.INTEGER),
                glue.Column(name='groupm_completed_3q_imps', type=glue.Schema.INTEGER),
                glue.Column(name='groupm_in_view_3q_imps', type=glue.Schema.INTEGER),
                glue.Column(name='groupm_completed_4q_imps', type=glue.Schema.INTEGER),
                glue.Column(name='groupm_in_view_4q_imps', type=glue.Schema.INTEGER),
                glue.Column(name='groupm_never_started_imps', type=glue.Schema.INTEGER),
                glue.Column(name='groupm_muted_imps', type=glue.Schema.INTEGER),
                glue.Column(name='groupm_full_screen_imps', type=glue.Schema.INTEGER),
                glue.Column(name='groupm_click_through_imps', type=glue.Schema.INTEGER),
                glue.Column(name='groupm_sivt_in_view_imps', type=glue.Schema.INTEGER),
                glue.Column(name='groupm_sivt_not_in_view_imps', type=glue.Schema.INTEGER),
                glue.Column(name='suspicious_imps', type=glue.Schema.INTEGER),
                glue.Column(name='measured_imps', type=glue.Schema.INTEGER),
                glue.Column(name='groupm_measured_imps', type=glue.Schema.INTEGER),
                glue.Column(name='general_invalid_imps', type=glue.Schema.INTEGER),
                glue.Column(name='viewability_measurement_trusted_imps', type=glue.Schema.INTEGER),
                glue.Column(name='imps', type=glue.Schema.INTEGER),
                glue.Column(name='sitting_duck_bot_imps', type=glue.Schema.INTEGER),
                glue.Column(name='standard_bot_imps', type=glue.Schema.INTEGER),
                glue.Column(name='volunteer_bot_imps', type=glue.Schema.INTEGER),
                glue.Column(name='profile_bot_imps', type=glue.Schema.INTEGER),
                glue.Column(name='masked_bot_imps', type=glue.Schema.INTEGER),
                glue.Column(name='nomadic_bot_imps', type=glue.Schema.INTEGER),
                glue.Column(name='other_bot_imps', type=glue.Schema.INTEGER),
                glue.Column(name='true_view_viewable_imps', type=glue.Schema.INTEGER),
                glue.Column(name='true_view_measurable_imps', type=glue.Schema.INTEGER),
                glue.Column(name='yahoo_gemini_billable_imps', type=glue.Schema.INTEGER),
                glue.Column(name='full_ad_in_view_imps', type=glue.Schema.INTEGER),
                glue.Column(name='pm_platform', type=glue.Schema.STRING),
                glue.Column(name='publicis_in_view_imps', type=glue.Schema.INTEGER),
                glue.Column(name='yahoo_gemini_billable_suspicious_imps', type=glue.Schema.INTEGER),
                glue.Column(name='average_in_view_time', type=glue.Schema.DOUBLE),
                glue.Column(name='in_view_lt_1s_imps', type=glue.Schema.INTEGER),
                glue.Column(name='in_view_1s_2s_imps', type=glue.Schema.INTEGER),
                glue.Column(name='in_view_2s_5s_imps', type=glue.Schema.INTEGER),
                glue.Column(name='in_view_5s_10s_imps', type=glue.Schema.INTEGER),
                glue.Column(name='in_view_10s_15s_imps', type=glue.Schema.INTEGER),
                glue.Column(name='in_view_15s_20s_imps', type=glue.Schema.INTEGER),
                glue.Column(name='in_view_20s_25s_imps', type=glue.Schema.INTEGER),
                glue.Column(name='in_view_25s_30s_imps', type=glue.Schema.INTEGER),
                glue.Column(name='in_view_30s_35s_imps', type=glue.Schema.INTEGER),
                glue.Column(name='in_view_35s_40s_imps', type=glue.Schema.INTEGER),
                glue.Column(name='in_view_40s_45s_imps', type=glue.Schema.INTEGER),
                glue.Column(name='in_view_45s_50s_imps', type=glue.Schema.INTEGER),
                glue.Column(name='in_view_ge_50s_imps', type=glue.Schema.INTEGER),
                glue.Column(name='viewability_measured_or_fraud_ads', type=glue.Schema.INTEGER)
            ],
            partition_keys=[
                glue.Column(name='estdate', type=glue.Schema.STRING)
            ],
            data_format=glue.DataFormat.TSV,
            bucket=bucket,
            s3_prefix=core.Fn.join('', [
                core.Fn.import_value(f'{SHARED_RESOURCES_STACK_NAME_BASE}-{app.env_id}:DataLakeMartBucketDataPrefix'),
                partner,
                '/'
            ]),
            compressed=True
        )

        # serialization properties
        cfn_table: glue.CfnTable = self.node.default_child
        cfn_table.add_property_override('TableInput.StorageDescriptor.SerdeInfo.Parameters', {
            'field.delim': '\t',
            'serialization.null.format': '\\N'
        })

        # data retention period
        cfn_table.add_property_override('TableInput.Retention', 365)
