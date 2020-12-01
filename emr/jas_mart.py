"""JAS mart aggregation script."""
# pylint: disable=no-name-in-module

from argparse import Namespace

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, LongType, StringType, DoubleType
from pyspark.sql.functions import (
    col, lit,
    to_date,
    sum as ssum, count
)

from common import PMISparkJob


class JASMartJob(PMISparkJob):
    """JAS mart aggregation job."""

    def __init__(self):
        super().__init__('JAS Mart Aggregation')

        self.add_argument('--partner', required=True)
        self.add_argument('--data-date', required=True)
        self.add_argument('--input-database-name', required=True)
        self.add_argument('--output-bucket-name', required=True)
        self.add_argument('--output-bucket-data-prefix', required=True)


    def execute(self, spark: SparkSession, args: Namespace):

        data_date_nodash = ''.join(args.data_date.split('-'))

        # aggregate the ad sessions
        metrics = spark \
            .table(f'{args.input_database_name}.{args.partner}') \
            .filter(col('estdate') == lit(data_date_nodash)) \
            .withColumn('hit_date', to_date(col('estdate'), 'yyyyMMdd')) \
            .drop('estdate') \
            .groupBy(
                'hit_date',
                'measurement_source_id',
                'partner_measured_advertiser_id',
                'partner_measured_campaign_id',
                'partner_measured_channel_id',
                'partner_measured_placement_id',
                'partner_measured_creative_id',
                'media_type_id'
            ) \
            .agg(
                ssum(col('below_the_fold').cast(IntegerType())).alias('below_the_fold_imps'),
                ssum(col('on_the_fold').cast(IntegerType())).alias('on_the_fold_imps'),
                ssum(col('above_the_fold').cast(IntegerType())).alias('above_the_fold_imps'),
                lit(None).cast(LongType()).alias('time_on_page'),
                lit(None).cast(LongType()).alias('in_view_time'),
                ssum(col('in_view').cast(IntegerType())).alias('in_view_imps'),
                ssum(col('in_view_5s').cast(IntegerType())).alias('in_view_5s_imps'),
                ssum(col('in_view_15s').cast(IntegerType())).alias('in_view_15s_imps'),
                ssum(col('not_in_view').cast(IntegerType())).alias('not_in_view_imps'),
                ssum(col('never_in_view').cast(IntegerType())).alias('never_in_view_imps'),
                ssum(col('in_view_load').cast(IntegerType())).alias('in_view_load_imps'),
                ssum(col('in_view_unload').cast(IntegerType())).alias('in_view_unload_imps'),
                ssum(col('completed_1q').cast(IntegerType())).alias('completed_1q_imps'),
                ssum(col('in_view_1q').cast(IntegerType())).alias('in_view_1q_imps'),
                ssum(col('completed_2q').cast(IntegerType())).alias('completed_2q_imps'),
                ssum(col('in_view_2q').cast(IntegerType())).alias('in_view_2q_imps'),
                ssum(col('completed_3q').cast(IntegerType())).alias('completed_3q_imps'),
                ssum(col('in_view_3q').cast(IntegerType())).alias('in_view_3q_imps'),
                ssum(col('completed_4q').cast(IntegerType())).alias('completed_4q_imps'),
                ssum(col('in_view_4q').cast(IntegerType())).alias('in_view_4q_imps'),
                ssum(col('never_started').cast(IntegerType())).alias('never_started_imps'),
                ssum(col('muted').cast(IntegerType())).alias('muted_imps'),
                ssum(col('full_screen').cast(IntegerType())).alias('full_screen_imps'),
                ssum(col('click_through').cast(IntegerType())).alias('click_through_imps'),
                ssum(col('sivt_in_view').cast(IntegerType())).alias('sivt_in_view_imps'),
                ssum(col('sivt_not_in_view').cast(IntegerType())).alias('sivt_not_in_view_imps'),
                lit(None).cast(LongType()).alias('groupm_time_on_page'),
                lit(None).cast(LongType()).alias('groupm_in_view_time'),
                ssum(col('groupm_in_view').cast(IntegerType())).alias('groupm_in_view_imps'),
                ssum(col('groupm_in_view_5s').cast(IntegerType())).alias('groupm_in_view_5s_imps'),
                ssum(col('groupm_in_view_15s').cast(IntegerType())).alias('groupm_in_view_15s_imps'),
                ssum(col('groupm_not_in_view').cast(IntegerType())).alias('groupm_not_in_view_imps'),
                ssum(col('groupm_never_in_view').cast(IntegerType())).alias('groupm_never_in_view_imps'),
                ssum(col('groupm_in_view_load').cast(IntegerType())).alias('groupm_in_view_load_imps'),
                ssum(col('groupm_in_view_unload').cast(IntegerType())).alias('groupm_in_view_unload_imps'),
                ssum(col('groupm_completed_1q').cast(IntegerType())).alias('groupm_completed_1q_imps'),
                ssum(col('groupm_in_view_1q').cast(IntegerType())).alias('groupm_in_view_1q_imps'),
                ssum(col('groupm_completed_2q').cast(IntegerType())).alias('groupm_completed_2q_imps'),
                ssum(col('groupm_in_view_2q').cast(IntegerType())).alias('groupm_in_view_2q_imps'),
                ssum(col('groupm_completed_3q').cast(IntegerType())).alias('groupm_completed_3q_imps'),
                ssum(col('groupm_in_view_3q').cast(IntegerType())).alias('groupm_in_view_3q_imps'),
                ssum(col('groupm_completed_4q').cast(IntegerType())).alias('groupm_completed_4q_imps'),
                ssum(col('groupm_in_view_4q').cast(IntegerType())).alias('groupm_in_view_4q_imps'),
                ssum(col('groupm_never_started').cast(IntegerType())).alias('groupm_never_started_imps'),
                ssum(col('groupm_muted').cast(IntegerType())).alias('groupm_muted_imps'),
                ssum(col('groupm_full_screen').cast(IntegerType())).alias('groupm_full_screen_imps'),
                ssum(col('groupm_click_through').cast(IntegerType())).alias('groupm_click_through_imps'),
                ssum(col('groupm_sivt_in_view').cast(IntegerType())).alias('groupm_sivt_in_view_imps'),
                ssum(col('groupm_sivt_not_in_view').cast(IntegerType())).alias('groupm_sivt_not_in_view_imps'),
                ssum(col('suspicious').cast(IntegerType())).alias('suspicious_imps'),
                ssum(col('measured').cast(IntegerType())).alias('measured_imps'),
                ssum(col('groupm_measured').cast(IntegerType())).alias('groupm_measured_imps'),
                ssum(col('general_invalid').cast(IntegerType())).alias('general_invalid_imps'),
                ssum(col('viewability_measurement_trusted').cast(IntegerType())).alias(
                    'viewability_measurement_trusted_imps'
                ),
                count('hit_date').alias('imps'),
                ssum(col('sitting_duck_bot').cast(IntegerType())).alias('sitting_duck_bot_imps'),
                ssum(col('standard_bot').cast(IntegerType())).alias('standard_bot_imps'),
                ssum(col('volunteer_bot').cast(IntegerType())).alias('volunteer_bot_imps'),
                ssum(col('profile_bot').cast(IntegerType())).alias('profile_bot_imps'),
                ssum(col('masked_bot').cast(IntegerType())).alias('masked_bot_imps'),
                ssum(col('nomadic_bot').cast(IntegerType())).alias('nomadic_bot_imps'),
                ssum(col('other_bot').cast(IntegerType())).alias('other_bot_imps'),
                ssum(col('true_view_viewable').cast(IntegerType())).alias('true_view_viewable_imps'),
                ssum(col('true_view_measurable').cast(IntegerType())).alias('true_view_measurable_imps'),
                ssum(col('yahoo_gemini_billable').cast(IntegerType())).alias('yahoo_gemini_billable_imps'),
                ssum(col('full_ad_in_view').cast(IntegerType())).alias('full_ad_in_view_imps'),
                lit(None).cast(StringType()).alias('pm_platform'),
                ssum(col('publicis_in_view').cast(IntegerType())).alias('publicis_in_view_imps'),
                ssum(col('yahoo_gemini_billable_suspicious').cast(IntegerType())).alias(
                    'yahoo_gemini_billable_suspicious_imps'
                ),
                lit(None).cast(DoubleType()).alias('average_in_view_time'),
                ssum(col('in_view_lt_1s').cast(IntegerType())).alias('in_view_lt_1s_imps'),
                ssum(col('in_view_1s_2s').cast(IntegerType())).alias('in_view_1s_2s_imps'),
                ssum(col('in_view_2s_5s').cast(IntegerType())).alias('in_view_2s_5s_imps'),
                ssum(col('in_view_5s_10s').cast(IntegerType())).alias('in_view_5s_10s_imps'),
                ssum(col('in_view_10s_15s').cast(IntegerType())).alias('in_view_10s_15s_imps'),
                ssum(col('in_view_15s_20s').cast(IntegerType())).alias('in_view_15s_20s_imps'),
                ssum(col('in_view_20s_25s').cast(IntegerType())).alias('in_view_20s_25s_imps'),
                ssum(col('in_view_25s_30s').cast(IntegerType())).alias('in_view_25s_30s_imps'),
                ssum(col('in_view_30s_35s').cast(IntegerType())).alias('in_view_30s_35s_imps'),
                ssum(col('in_view_35s_40s').cast(IntegerType())).alias('in_view_35s_40s_imps'),
                ssum(col('in_view_40s_45s').cast(IntegerType())).alias('in_view_40s_45s_imps'),
                ssum(col('in_view_45s_50s').cast(IntegerType())).alias('in_view_45s_50s_imps'),
                ssum(col('in_view_ge_50s').cast(IntegerType())).alias('in_view_ge_50s_imps'),
                ssum(col('viewability_measured_or_fraud').cast(IntegerType())) \
                    .alias('viewability_measured_or_fraud_ads')
            )

        # save metrics
        metrics \
            .write \
            .mode('overwrite') \
            .csv(
                (
                    f's3://{args.output_bucket_name}/{args.output_bucket_data_prefix}'
                    f'/{args.partner}/date={data_date_nodash}/'
                ),
                compression='gzip',
                sep='\t',
                nullValue='\\N',
                dateFormat='yyyy-MM-dd'
            )


# execute the job
if __name__ == '__main__':
    JASMartJob().run()
