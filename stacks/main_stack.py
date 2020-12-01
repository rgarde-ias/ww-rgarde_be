"""Main stack module."""

from aws_cdk import (
    aws_iam as iam,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    core
)

from ias_pmi_cdk_common import PMIApp, get_region_designator

from .constants import SHARED_RESOURCES_STACK_NAME_BASE
from .glue_catalog_construct import GlueCatalogConstruct


class MainStack(core.Stack):
    """Main stack."""

    def __init__(self, scope: core.Construct, app: PMIApp, cid: str):
        super().__init__(
            scope, cid,
            env=app.env,
            stack_name=app.safe_app_name,
            description="AWS resources for PM pipeline backend."
        )

        # get region designator to use in global resources names
        region_designator = get_region_designator(self.region)

        # datalake buckets
        raw_bucket = s3.Bucket.from_bucket_name(
            self, 'DataLakeRawBucket',
            core.Fn.import_value(f'{SHARED_RESOURCES_STACK_NAME_BASE}-{app.env_id}:DataLakeRawBucketName')
        )
        jas_bucket = s3.Bucket.from_bucket_name(
            self, 'DataLakeJasBucket',
            core.Fn.import_value(f'{SHARED_RESOURCES_STACK_NAME_BASE}-{app.env_id}:DataLakeJASBucketName')
        )
        jas_mart_bucket = s3.Bucket.from_bucket_name(
            self, 'DataLakeJasMartBucket',
            core.Fn.import_value(f'{SHARED_RESOURCES_STACK_NAME_BASE}-{app.env_id}:DataLakeMartBucketName')
        )

        # intermidiate results S3 bucket
        intermediate_bucket = s3.Bucket(
            self, 'IntermediateBucket',
            bucket_name=f'iastf-pipeline-intermediate-{region_designator}-pmi-{app.env_id}',
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=core.RemovalPolicy.DESTROY
        )
        intermediate_bucket.add_lifecycle_rule(
            id='expire',
            expiration=core.Duration.days(14),
            abort_incomplete_multipart_upload_after=core.Duration.days(3)
        )

        # EMR service role
        iam.Role(
            self, 'EmrServiceRole',
            role_name=f'{app.safe_app_name}-{region_designator}-emr',
            description="PM pipeline back-end EMR service role",
            assumed_by=iam.ServicePrincipal('elasticmapreduce.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AmazonElasticMapReduceRole')
            ]
        )

        # EMR job role
        emr_job_role = iam.Role(
            self, 'EmrJobRole',
            role_name=f'{app.safe_app_name}-{region_designator}-emr-job',
            description="PM pipeline back-end EMR job role",
            assumed_by=iam.ServicePrincipal('ec2.amazonaws.com')
        )
        emr_job_role.add_to_principal_policy(iam.PolicyStatement(
            sid='AllowReadEMRJobAssets',
            actions=['s3:GetObject'],
            resources=[
                self.format_arn(
                    service='s3', region='', account='',
                    resource=f'iasrf-vcs-mixed-{region_designator}-de-{app.env_id}',
                    resource_name='*'
                )
            ]
        ))
        emr_job_role.add_to_principal_policy(iam.PolicyStatement(
            sid='AllowWriteEMTJobLogs',
            actions=['s3:PutObject'],
            resources=[
                self.format_arn(
                    service='s3', region='', account='',
                    resource=f'iaspl-pipeline-logging-{region_designator}-de-{app.env_id}',
                    resource_name='emr_logs/*'
                )
            ]
        ))
        emr_job_role.add_to_principal_policy(iam.PolicyStatement(
            sid='AllowReadGlueDatabases',
            actions=[
                'glue:GetDatabase',
                'glue:GetTable',
                'glue:GetPartitions',
                'glue:GetUserDefinedFunctions'
            ],
            resources=[
                self.format_arn(service='glue', resource='catalog'),
                self.format_arn(service='glue', resource='database', resource_name='*'),
                self.format_arn(service='glue', resource='table', resource_name='*')
            ]
        ))
        raw_bucket.grant_read(emr_job_role)
        jas_bucket.grant_read_write(emr_job_role)
        jas_mart_bucket.grant_read_write(emr_job_role)
        intermediate_bucket.grant_read_write(emr_job_role)
        iam.CfnInstanceProfile(
            self, 'EmrInstanceProfile',
            instance_profile_name=emr_job_role.role_name,
            roles=[
                emr_job_role.role_name
            ]
        )

        # Glue catalog
        GlueCatalogConstruct(
            self, app, 'Glue',
            jas_bucket=jas_bucket,
            jas_mart_bucket=jas_mart_bucket
        )

        # pipeline state DynamoDB table
        pipeline_state_table = dynamodb.Table(
            self, 'PipelineStateTable',
            table_name=f'{app.safe_app_name}-state',
            partition_key=dynamodb.Attribute(
                name='stateKey',
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=core.RemovalPolicy.DESTROY
        )

        # stack outputs
        core.CfnOutput(
            self, 'IntermediateBucketName',
            description="Name of the S3 bucket for intermediate pipeline data.",
            value=intermediate_bucket.bucket_name
        )
        core.CfnOutput(
            self, 'PipelineStateTableName',
            description="Name of the DynamoDB table used for storing the pipeline state between runs.",
            value=pipeline_state_table.table_name
        )
