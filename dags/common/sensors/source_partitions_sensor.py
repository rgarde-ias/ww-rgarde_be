"""Glue table partitions support module."""

from typing import Mapping, Dict, List, Any
from datetime import datetime, timezone, timedelta

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_glue_catalog_hook import AwsGlueCatalogHook

from ..pmi_dag import PMIDAG


class SourcePartitionsSensor(BaseSensorOperator):
    """Sensor implementation for source partitions availability.

    The sensor makes sure that data for all 24 hours of the given data date are
    available in the raw events data source. It does it by checking the presence
    of the corresponding partitions in the partner's raw events Glue table.

    Parameters
    ----------
    partner
        The partner.
    args, kwargs
        Standard Airflow sensor arguments.

    """

    ui_color = '#C5CAE9'

    TIMEOUT_HOURS = 3


    @apply_defaults
    def __init__(
            self,
            *args,
            partner: str,
            **kwargs
    ):
        super().__init__(
            poke_interval=3 * 60,
            timeout=SourcePartitionsSensor.TIMEOUT_HOURS * 3600,
            *args, **kwargs
        )

        dag = PMIDAG.get_dag(**kwargs)

        self._database = dag.env_config['raw_events_glue_db_name']
        self._table = partner

        self._glue_hook = AwsGlueCatalogHook(
            aws_conn_id=dag.aws_conn_id
        )


    def poke(self, context: Mapping[str, Any]) -> bool:

        # get partitions matching the data date
        utcdate_base = datetime(
            *(int(p) for p in context['ds'].split('-')),
            tzinfo=PMIDAG.DATA_DATE_TZ
        ).astimezone(timezone.utc)
        partitions: Dict[str, List[str]] = {}
        for hour in range(24):
            utcdatehour = utcdate_base + timedelta(hours=hour)
            utcdate = utcdatehour.strftime('%Y%m%d')
            utchour = utcdatehour.strftime('%H')
            if utcdate in partitions:
                partitions[utcdate].append(utchour)
            else:
                partitions[utcdate] = [utchour]

        # build test expression
        expression = ' OR '.join(
            (
                f"(utcdate = '{utcdate}' AND utchour BETWEEN '{utchours[0]}' AND '{utchours[-1]}')"
            ) for utcdate, utchours in partitions.items()
        )

        # query partitions
        self.log.info(
            "Poking for table %s.%s, expression %s",
            self._database, self._table, expression
        )
        partition_descs = self._glue_hook.get_partitions(
            database_name=self._database,
            table_name=self._table,
            expression=expression,
            max_items=24
        )

        # check if all 24 hours available
        return len(partition_descs) == 24
