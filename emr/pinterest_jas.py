"""JAS aggregation PySpark script for Pinterest."""
# pylint: disable=no-name-in-module

from argparse import Namespace

from pyspark.sql import SparkSession, Column
from pyspark.sql.types import LongType, IntegerType, StringType, BooleanType, DoubleType
from pyspark.sql.functions import (
    col, lit, when,
    first, collect_set, max as smax,
    array_contains, lpad
)

from common import BaseJASJob, get_source_partitions_condition


def _event_prop(event_type: str, expr: Column) -> Column:
    """Get property from the event of a certain type within the ad session.

    Parameters
    ----------
    event_type
        Event type.
    expr
        Value expression.

    Returns
    -------
    Column
        Column expression that evaluates to the provided `expr` if the event
        type matches the specified one, or None otherwise.

    """
    return first(
        when(col('type') == event_type, expr),
        ignorenulls=True
    )


class PinterestJASJob(BaseJASJob):
    """JAS aggregation job for Pinterest."""

    def __init__(self):
        super().__init__('JAS Aggregation for Pinterest', 'pinterest')


    def execute(self, spark: SparkSession, args: Namespace):

        # select and cleanup the raw events for the data date
        events = spark \
            .table(f'{args.input_database_name}.{self.partner}') \
            .withColumnRenamed('impressionid', 'impression_id') \
            .withColumnRenamed('sourceid', 'measurement_source_id') \
            .withColumnRenamed('advertiserid', 'partner_measured_advertiser_id') \
            .withColumnRenamed('campaignid', 'partner_measured_campaign_id') \
            .withColumn('partner_measured_channel_id', lit(None).cast(LongType())) \
            .withColumnRenamed('placementid', 'partner_measured_placement_id') \
            .withColumnRenamed('creativeid', 'partner_measured_creative_id') \
            .withColumnRenamed('mediatypeid', 'media_type_id') \
            .withColumnRenamed('inview50', 'in_view_50') \
            .withColumnRenamed('inview100', 'in_view_100') \
            .withColumnRenamed('noncontinuousplaytime', 'non_continuous_play_time') \
            .withColumnRenamed('audibleplaytime', 'audible_play_time') \
            .withColumnRenamed('fullscreen', 'full_screen') \
            .withColumnRenamed('fraudcategory', 'fraud_category') \
            .withColumnRenamed('givt', 'general_invalid') \
            .filter(get_source_partitions_condition(
                args.data_date, args.data_date_tz_offset
            )) \
            .dropDuplicates(['impression_id', 'type', 'timestamp'])

        # assemble ad sessions
        adsessions = events \
            .groupBy('impression_id', 'timestamp') \
            .agg(
                first(lpad(
                    (
                        (
                            col('utchour').cast(IntegerType())
                            + lit(24 + args.data_date_tz_offset / 3600).cast(IntegerType())
                        ) % lit(24)
                    ).cast(StringType()),
                    2, '0'
                )).alias('hour'),
                collect_set('type').alias('events'),
                _event_prop('impression', col('site')).alias('site'),
                _event_prop('impression', col('measurement_source_id')).alias('measurement_source_id'),
                _event_prop('impression', col('partner_measured_advertiser_id')) \
                    .alias('partner_measured_advertiser_id'),
                _event_prop('impression', col('partner_measured_campaign_id')).alias('partner_measured_campaign_id'),
                _event_prop('impression', col('partner_measured_channel_id')).alias('partner_measured_channel_id'),
                _event_prop('impression', col('partner_measured_placement_id')).alias('partner_measured_placement_id'),
                _event_prop('impression', col('partner_measured_creative_id')).alias('partner_measured_creative_id'),
                _event_prop('impression', col('media_type_id')).alias('media_type_id'),
                _event_prop('impression', col('endpoint')).alias('endpoint'),
                _event_prop('impression', col('suspicious')).alias('suspicious'),
                _event_prop('impression', col('general_invalid')).alias('general_invalid'),
                _event_prop('impression', col('fraud_category')).alias('fraud_category'),
                _event_prop('impression', (col('measurable') == 1) | (col('measurable') == 3)) \
                    .alias('mrc_measurable'),
                _event_prop('impression', (col('measurable') == 2) | (col('measurable') == 3)) \
                    .alias('groupm_measurable'),
                first(
                    when(
                        (
                            ((col('endpoint') == 'display') & (col('type') == 'impression'))
                            | ((col('endpoint') == 'video') & (col('type') == 'started'))
                        ),
                        col('in_view_50')
                    ),
                    ignorenulls=True
                ).alias('mrc_in_view_on_load'),
                _event_prop('unload', col('in_view_50')).alias('mrc_in_view_on_unload'),
                _event_prop('first-quartile', col('in_view_50')).alias('mrc_in_view_1q'),
                _event_prop('second-quartile', col('in_view_50')).alias('mrc_in_view_2q'),
                _event_prop('thrid-quartile', col('in_view_50')).alias('mrc_in_view_3q'),
                _event_prop('completed', col('in_view_50')).alias('mrc_in_view_4q'),
                _event_prop('started', col('in_view_100')).alias('groupm_in_view_on_load'),
                _event_prop('unload', col('in_view_100')).alias('groupm_in_view_on_unload'),
                _event_prop('first-quartile', col('in_view_100')).alias('groupm_in_view_1q'),
                _event_prop('second-quartile', col('in_view_100')).alias('groupm_in_view_2q'),
                _event_prop('thrid-quartile', col('in_view_100')).alias('groupm_in_view_3q'),
                _event_prop('completed', col('in_view_100')).alias('groupm_in_view_4q'),
                ((smax('non_continuous_play_time') - smax('audible_play_time')) > lit(1000)).alias('muted'),
                _event_prop('impression', col('full_screen')).alias('full_screen')
            ) \
            .filter(array_contains(col('events'), 'impression')) \
            .withColumn('date', lit(''.join(args.data_date.split('-')))) \
            .withColumn('mrc_viewable', (
                col('mrc_measurable') & (
                    ((col('endpoint') == 'display') & array_contains(col('events'), 'in-view'))
                    | ((col('endpoint') == 'video') & array_contains(col('events'), 'in-view-50'))
                )
            )) \
            .withColumn('groupm_viewable', (
                col('groupm_measurable') & array_contains(col('events'), '100-percent-in-view')
            )) \
            .withColumn('clean', (
                ~col('suspicious') & ~col('general_invalid')
            )) \
            .select(
                'date',
                'hour',
                'impression_id',
                'site',
                'measurement_source_id',
                'partner_measured_advertiser_id',
                'partner_measured_campaign_id',
                'partner_measured_channel_id',
                'partner_measured_placement_id',
                'partner_measured_creative_id',
                'media_type_id',
                lit(None).cast(BooleanType()).alias('below_the_fold'),
                lit(None).cast(BooleanType()).alias('on_the_fold'),
                lit(None).cast(BooleanType()).alias('above_the_fold'),
                lit(None).cast(IntegerType()).alias('time_on_page'),
                lit(None).cast(IntegerType()).alias('in_view_time'),
                (col('clean') & col('mrc_viewable')).alias('in_view'),
                (col('clean') & array_contains(col('events'), 'in-view-5s')).alias('in_view_5s'),
                (col('clean') & array_contains(col('events'), 'in-view-15s')).alias('in_view_15s'),
                (col('clean') & (
                    ((col('endpoint') == 'display') & ~array_contains(col('events'), 'in-view'))
                    | ((col('endpoint') == 'video') & ~array_contains(col('events'), 'in-view-50'))
                )).alias('not_in_view'),
                lit(None).cast(BooleanType()).alias('never_in_view'),
                (col('clean') & col('mrc_measurable') & col('mrc_in_view_on_load')).alias('in_view_load'),
                (col('clean') & col('mrc_measurable') & col('mrc_in_view_on_unload')).alias('in_view_unload'),
                (col('clean') & array_contains(col('events'), 'first-quartile')).alias('completed_1q'),
                (col('clean') & col('mrc_measurable') & col('mrc_in_view_1q')).alias('in_view_1q'),
                (col('clean') & array_contains(col('events'), 'second-quartile')).alias('completed_2q'),
                (col('clean') & col('mrc_measurable') & col('mrc_in_view_2q')).alias('in_view_2q'),
                (col('clean') & array_contains(col('events'), 'thrid-quartile')).alias('completed_3q'),
                (col('clean') & col('mrc_measurable') & col('mrc_in_view_3q')).alias('in_view_3q'),
                (col('clean') & array_contains(col('events'), 'completed')).alias('completed_4q'),
                (col('clean') & col('mrc_measurable') & col('mrc_in_view_4q')).alias('in_view_4q'),
                (col('clean') & ~array_contains(col('events'), 'started')).alias('never_started'),
                (col('clean') & col('muted')).alias('muted'),
                (col('clean') & col('full_screen')).alias('full_screen'),
                lit(False).alias('click_through'),
                (~col('clean') & col('mrc_viewable')).alias('sivt_in_view'),
                (~col('clean') & ~col('mrc_viewable')).alias('sivt_not_in_view'),
                lit(None).cast(IntegerType()).alias('groupm_time_on_page'),
                lit(None).cast(IntegerType()).alias('groupm_in_view_time'),
                (col('clean') & col('groupm_viewable')).alias('groupm_in_view'),
                lit(False).alias('groupm_in_view_5s'),
                lit(False).alias('groupm_in_view_15s'),
                (col('clean') & ~col('groupm_viewable')).alias('groupm_not_in_view'),
                lit(False).alias('groupm_never_in_view'),
                (col('clean') & col('groupm_measurable') & col('groupm_in_view_on_load')).alias('groupm_in_view_load'),
                (col('clean') & col('groupm_measurable') & col('groupm_in_view_on_unload')) \
                    .alias('groupm_in_view_unload'),
                (col('clean') & array_contains(col('events'), 'first-quartile')).alias('groupm_completed_1q'),
                (col('clean') & col('groupm_measurable') & col('groupm_in_view_1q')).alias('groupm_in_view_1q'),
                (col('clean') & array_contains(col('events'), 'second-quartile')).alias('groupm_completed_2q'),
                (col('clean') & col('groupm_measurable') & col('groupm_in_view_2q')).alias('groupm_in_view_2q'),
                (col('clean') & array_contains(col('events'), 'thrid-quartile')).alias('groupm_completed_3q'),
                (col('clean') & col('groupm_measurable') & col('groupm_in_view_3q')).alias('groupm_in_view_3q'),
                (col('clean') & array_contains(col('events'), 'completed')).alias('groupm_completed_4q'),
                (col('clean') & col('groupm_measurable') & col('groupm_in_view_4q')).alias('groupm_in_view_4q'),
                (col('clean') & ~array_contains(col('events'), 'started')).alias('groupm_never_started'),
                (col('clean') & col('muted')).alias('groupm_muted'),
                (col('clean') & col('full_screen')).alias('groupm_full_screen'),
                lit(False).alias('groupm_click_through'),
                (~col('clean') & col('groupm_viewable')).alias('groupm_sivt_in_view'),
                (~col('clean') & ~col('groupm_viewable')).alias('groupm_sivt_not_in_view'),
                'suspicious',
                lit(None).cast(BooleanType()).alias('measured'),
                lit(None).cast(BooleanType()).alias('groupm_measured'),
                'general_invalid',
                col('mrc_measurable').alias('viewability_measurement_trusted'),
                (col('suspicious') & (col('fraud_category') == lit('Sitting Duck'))).alias('sitting_duck_bot'),
                (col('suspicious') & (col('fraud_category') == lit('Standard'))).alias('standard_bot'),
                (col('suspicious') & (col('fraud_category') == lit('Volunteer'))).alias('volunteer_bot'),
                (col('suspicious') & (col('fraud_category') == lit('Profile'))).alias('profile_bot'),
                (col('suspicious') & (col('fraud_category') == lit('Masked'))).alias('masked_bot'),
                (col('suspicious') & (col('fraud_category') == lit('Nomadic'))).alias('nomadic_bot'),
                (col('suspicious') & (col('fraud_category') == lit('Other'))).alias('other_bot'),
                lit(None).cast(BooleanType()).alias('true_view_viewable'),
                lit(None).cast(BooleanType()).alias('true_view_measurable'),
                lit(None).cast(BooleanType()).alias('yahoo_gemini_billable'),
                (col('clean') & col('groupm_measurable') & array_contains(
                    col('events'), '100-percent-in-view-2-seconds'
                )).alias('full_ad_in_view'),
                (col('clean') & col('groupm_measurable') & array_contains(
                    col('events'), '100-percent-in-view-2-seconds'
                )).alias('publicis_in_view'),
                lit(None).cast(BooleanType()).alias('yahoo_gemini_billable_suspicious'),
                lit(None).cast(DoubleType()).alias('average_in_view_time'),
                lit(None).cast(BooleanType()).alias('in_view_lt_1s'),
                lit(None).cast(BooleanType()).alias('in_view_1s_2s'),
                lit(None).cast(BooleanType()).alias('in_view_2s_5s'),
                lit(None).cast(BooleanType()).alias('in_view_5s_10s'),
                lit(None).cast(BooleanType()).alias('in_view_10s_15s'),
                lit(None).cast(BooleanType()).alias('in_view_15s_20s'),
                lit(None).cast(BooleanType()).alias('in_view_20s_25s'),
                lit(None).cast(BooleanType()).alias('in_view_25s_30s'),
                lit(None).cast(BooleanType()).alias('in_view_30s_35s'),
                lit(None).cast(BooleanType()).alias('in_view_35s_40s'),
                lit(None).cast(BooleanType()).alias('in_view_40s_45s'),
                lit(None).cast(BooleanType()).alias('in_view_45s_50s'),
                lit(None).cast(BooleanType()).alias('in_view_ge_50s'),
                (col('mrc_measurable') | col('groupm_measurable') | ~col('clean')) \
                    .alias('viewability_measured_or_fraud')
            )

        # save ad sessions
        adsessions \
            .write \
            .mode('overwrite') \
            .partitionBy('date', 'hour') \
            .format('avro') \
            .option('compression', 'snappy') \
            .save(
                f's3://{args.output_bucket_name}/{args.output_bucket_data_prefix}'
                f'/{self.partner}/'
            )


# execute the job
if __name__ == '__main__':
    PinterestJASJob().run()
