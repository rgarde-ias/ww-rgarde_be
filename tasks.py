"""Project tasks.

See https://www.pyinvoke.org/ for details.

"""

import os
import json

from invoke import task
import chef
import boto3


APP_NAME = 'etl-pm-pipeline-be-rgarde'

CHEF_API_URL = 'http://chef.mars.303net.pvt/organizations/default'
CHEF_API_USER = 'gd-deploy'
AIRFLOW_SSH_USER = 'gd-deploy'


@task
def validate(ctx):
    """Validate the project codebase."""

    # lint Python code
    print("linting Python code...")
    ctx.run('pylint tasks.py app.py stacks pipeline.py dags emr/**/*.py')

    # validate envs config
    print("validating envs config file...")
    ctx.run('jsonschema -i envs-config.json envs-config.schema.json')

    # attempt to synth the CDK app
    for env_id in ('dev', 'staging', 'prod'):
        print(f"synthesizing CloudFormation templates for {env_id}...")
        ctx.run(
            'cdk synth',
            env={
                'CDK_DEPLOY_ENV': env_id,
                'CDK_DEPLOY_ACCOUNT': '000000000000',
                'CDK_DEPLOY_REGION': 'us-east-1'
            },
            hide='out'
        )


@task
def test(ctx):
    """Run unit tests."""

    # run unit tests
    print("running unit tests...")
    ctx.run('pytest tests/')


@task(help={
    'env-id': "Deployment environment id (dev, staging, prod, etc.).",
    'env-config-json': "Optional environment configuration JSON string for non-standard environments."
})
def deploy(ctx, env_id, env_config_json=None):
    """Deploy project to AWS."""

    # get target deployment environment configuration
    if env_config_json is not None:
        print("using provided target deployment environment configuration...")
        env_config = json.loads(env_config_json)
    else:
        print("loading target deployment environment configuration...")
        with open('envs-config.json') as envs_config_file:
            env_config = json.load(envs_config_file)[env_id]

    # get AWS account id
    print("getting AWS account id...")
    aws_account_id = boto3.client('sts').get_caller_identity()['Account']

    # get shared resources stack outputs
    print("getting shared AWS resources stack outputs...")
    cloudformation = boto3.client('cloudformation', region_name=env_config['region'])
    res = cloudformation.describe_stacks(StackName=f'etl-pm-shared-{env_id}')
    aws_resources = {
        **{v['OutputKey']: v['OutputValue'] for v in res['Stacks'][0]['Outputs']}
    }

    # deploy CDK app
    print("deploying AWS resources...")
    ctx.run(
        (
            f'cdk deploy'
            f' --role-arn arn:aws:iam::{aws_account_id}:role/IAS-CloudFormation-Deploy-Role'
            f' --require-approval never'
        ),
        env={
            'CDK_DEPLOY_ENV': env_id,
            'CDK_DEPLOY_REGION': env_config['region'],
            'CDK_DEPLOY_ENV_CONFIG': json.dumps(env_config)
        }
    )

    # get backend resources stack outputs
    print("getting backend AWS resources stack outputs...")
    res = cloudformation.describe_stacks(StackName=f'{APP_NAME}-{env_id}')
    aws_resources.update({
        **{v['OutputKey']: v['OutputValue'] for v in res['Stacks'][0]['Outputs']}
    })

    # prepare EMR assets package
    print("assembling EMR assets package...")
    with ctx.cd('emr'):
        ctx.run(
            'rm -rf ../build/assets/emr'
            ' && mkdir -p ../build/assets/emr'
            ' && cp *.py ../build/assets/emr'
            ' && zip -r ../build/assets/emr/packages.zip $(ls -d */) -i \\*.py'
        )

    # upload assets to S3
    print("uploading assets to S3...")
    with open('version.properties') as version_file:
        deployment_version = [
            v.rstrip().split('=')[1] for v in version_file if v.startswith('version=')
        ][0]
    region_designator = ''.join(part[0] for part in env_config['region'].split('-'))
    ctx.run(
        f'aws s3 sync --delete build/assets/'
        f' s3://iasrf-vcs-mixed-{region_designator}-de-{env_id}/pm/airflow/{APP_NAME}/v{deployment_version}/'
    )

    # prepare DAGs package
    print("assembling DAGs package...")
    ctx.run(
        f'mkdir -p build/airflow'
        f' && rm -f build/airflow/{APP_NAME}-{env_id}.zip'
        f' && zip -r build/airflow/{APP_NAME}-{env_id}.zip pipeline.py dags -i \\*.py'
    )
    env_config['env_id'] = env_id
    env_config['deployment_version'] = deployment_version
    env_config['alarms_topic_name'] = aws_resources['AlarmsTopicName']
    env_config['notifications_topic_name'] = aws_resources['NotificationsTopicName']
    env_config['jas_bucket_name'] = aws_resources['DataLakeJASBucketName']
    env_config['jas_bucket_data_prefix'] = aws_resources['DataLakeJASBucketDataPrefix']
    env_config['jas_mart_bucket_name'] = aws_resources['DataLakeMartBucketName']
    env_config['jas_mart_bucket_data_prefix'] = aws_resources['DataLakeMartBucketDataPrefix']
    env_config['itermediate_bucket_name'] = aws_resources['IntermediateBucketName']
    env_config['pipeline_state_table_name'] = aws_resources['PipelineStateTableName']
    with open('build/airflow/env-config.json', 'w') as env_config_file:
        json.dump(env_config, env_config_file, indent=2)
    with ctx.cd('build/airflow'):
        ctx.run(
            f'zip -g {APP_NAME}-{env_id}.zip env-config.json'
        )

    # get Airflow nodes
    print("getting Airflow nodes from Chef...")
    with chef.ChefAPI(CHEF_API_URL, os.environ['CHEF_API_KEY'], CHEF_API_USER):
        airflow_nodes = [
            node.object.name for node in chef.Search(
                'node',
                f"tags:{env_config['airflow_cluster_id']}.{env_id}"
            )
        ]
    print(f"got Airflow nodes: {airflow_nodes}")

    # upload the DAGs package to Airflow nodes
    for node in airflow_nodes:
        print(f"deploying DAGs package to {node}...")
        ctx.run(
            f'scp -o StrictHostKeyChecking=no build/airflow/{APP_NAME}-{env_id}.zip'
            f' {AIRFLOW_SSH_USER}@{node}:/var/lib/airflow/dags/'
        )
