"""Script for extracting new campaign id mappings from raw events."""
# pylint: disable=no-name-in-module

from argparse import Namespace

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit
)

from common import PMISparkJob, get_source_partitions_condition


class CampaignIDMappingsJob(PMISparkJob):
    """Campaign ID mappings extraction job."""

    def __init__(self):
        super().__init__('Campaign ID Mappings Extraction')

        self.add_argument('--partner', required=True)
        self.add_argument('--data-date', required=True)
        self.add_argument('--data-date-tz-offset', type=int, required=True)
        self.add_argument('--input-database-name', required=True)
        self.add_argument('--output-bucket-name', required=True)
        self.add_argument('--output-bucket-data-prefix', required=True)


    def execute(self, spark: SparkSession, args: Namespace):

        # select raw events for the data date
        events = spark \
            .table(f'{args.input_database_name}.{args.partner}') \
            .withColumnRenamed('campaignid', 'partner_measured_campaign_id') \
            .withColumnRenamed('iasadventityid', 'ias_adv_entity_id') \
            .filter(get_source_partitions_condition(args.data_date, args.data_date_tz_offset))

        # get campaign id mappings
        mappings = events \
            .filter(col('ias_adv_entity_id') != lit(0)) \
            .select('partner_measured_campaign_id', 'ias_adv_entity_id') \
            .distinct()

        # save the mappings
        data_date_nodash = ''.join(args.data_date.split('-'))
        mappings \
            .write \
            .mode('overwrite') \
            .json(
                f's3://{args.output_bucket_name}/{args.output_bucket_data_prefix}'
                f'/{args.partner}/date={data_date_nodash}/'
            )


# execute the job
if __name__ == '__main__':
    CampaignIDMappingsJob().run()
