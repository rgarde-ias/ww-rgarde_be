"""Glue catalog objects construct module."""

from aws_cdk import (
    aws_glue as glue,
    aws_s3 as s3,
    core
)

from ias_pmi_cdk_common import PMIApp

from .constants import ALL_PARTNERS
from .jas_glue_table import JasGlueTable
from .jas_mart_glue_table import JasMartGlueTable


class GlueCatalogConstruct(core.Construct):
    """Glue catalog objects construct."""

    def __init__(
            self, scope: core.Construct, app: PMIApp, cid: str,
            *,
            jas_bucket: s3.IBucket,
            jas_mart_bucket: s3.IBucket
    ):
        super().__init__(scope, cid)

        # Glue database references
        jas_database = glue.Database.from_database_arn(
            self, 'JasDatabase',
            core.Stack.of(scope).format_arn(
                service='glue',
                resource='database',
                resource_name=app.env_config['jas_glue_db_name']
            )
        )
        jas_mart_database = glue.Database.from_database_arn(
            self, 'JasMartDatabase',
            core.Stack.of(scope).format_arn(
                service='glue',
                resource='database',
                resource_name=app.env_config['jas_mart_glue_db_name']
            )
        )

        # Glue tables for each partner
        for partner in ALL_PARTNERS:
            JasGlueTable(
                self, app, f'JasTablefor{partner}',
                partner=partner,
                bucket=jas_bucket,
                database=jas_database
            )
            JasMartGlueTable(
                self, app, f'JasMartTablefor{partner}',
                partner=partner,
                bucket=jas_mart_bucket,
                database=jas_mart_database
            )
