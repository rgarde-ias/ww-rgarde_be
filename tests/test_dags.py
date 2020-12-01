# pylint: disable=missing-module-docstring, missing-function-docstring, missing-class-docstring

import os.path
from datetime import datetime

import requests

import pytest

from airflow import DAG


@pytest.fixture
def mock_requests(monkeypatch):

    class MockInstanceIdentityDocumentResponse:
        def json(self):
            return {
                'region': 'us-east-1',
                'accountId': '000000000000'
            }

    real_requests_get = requests.get
    def mock_requests_get(url):
        if url == 'http://169.254.169.254/latest/dynamic/instance-identity/document':
            return MockInstanceIdentityDocumentResponse()
        return real_requests_get(url)

    monkeypatch.setattr(requests, 'get', mock_requests_get)


@pytest.fixture
def env_config():

    return {
        'env_id': 'test',
        'deployment_version': '0.0.0',
        'alarms_topic_name': 'alarms',
        'notifications_topic_name': 'notifications',
        'jas_bucket_name': 'jas-bucket',
        'jas_bucket_data_prefix': 'jas_prefix',
        'jas_mart_bucket_name': 'jas-mart-bucket',
        'jas_mart_bucket_data_prefix': 'jas_mart_prefix',
        'itermediate_bucket_name': 'intermediate-bucket',
        'pipeline_state_table_name': 'pipeline-state-table',
        'region': 'us-east-1',
        'airflow_cluster_id': 'ariflow.test.cluster',
        'airflow_daily_schedule': None,
        'airflow_hourly_schedule': None,
        'airflow_aws_conn_id': 'aws_default',
        'airflow_firewall_db_conn_id': 'firewall_db_rw',
        'emr_cluster_account': 'ias-dev',
        'emr_cluster_instance_type': 'm5.2xlarge',
        'emr_cluster_instance_count': 1,
        'raw_events_glue_db_name': 'partner_raw',
        'jas_glue_db_name': 'partner_jas',
        'jas_mart_glue_db_name': 'partner_jas_mart',
        'data_load_trigger_sns_topic_arn': 'arn:aws:sns:us-east-1:000000000000:etl-pm-s3-dw-ingestion-trigger',
        'sftp_bucket_name': 'sftp-bucket',
        'partners': {
            'facebook': {
                'airflow_schedule': None,
            },
        },
    }


def test_daily_dag(request, monkeypatch, env_config, mock_requests):
    # pylint: disable=redefined-outer-name, unused-argument

    monkeypatch.syspath_prepend(os.path.normpath(f'{request.fspath}/../..'))

    from dags import DailyDAG # pylint: disable=import-error, import-outside-toplevel

    daily_dag = DailyDAG('test-daily', env_config, datetime(2020, 8, 1))

    assert isinstance(daily_dag, DAG)
    assert daily_dag.env_config['env_id'] == 'test'
    assert daily_dag.aws_env['region'] == 'us-east-1'
    assert daily_dag.aws_env['accountId'] == '000000000000'


def test_hourly_dag(request, monkeypatch, env_config, mock_requests):
    # pylint: disable=redefined-outer-name, unused-argument

    monkeypatch.syspath_prepend(os.path.normpath(f'{request.fspath}/../..'))

    from dags import HourlyDAG # pylint: disable=import-error, import-outside-toplevel

    hourly_dag = HourlyDAG('test-hourly', env_config, datetime(2020, 8, 1))

    assert isinstance(hourly_dag, DAG)


def test_facebook_dag(request, monkeypatch, env_config, mock_requests):
    # pylint: disable=redefined-outer-name, unused-argument

    monkeypatch.syspath_prepend(os.path.normpath(f'{request.fspath}/../..'))

    from dags import FacebookDAG  # pylint: disable=import-error, import-outside-toplevel

    facebook_dag = FacebookDAG('test-facebook', env_config, datetime(2020, 8, 1))

    assert isinstance(facebook_dag, DAG)
