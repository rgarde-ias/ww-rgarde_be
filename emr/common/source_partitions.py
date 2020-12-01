"""Common functionality for source partitions."""

from typing import Dict, List
from datetime import datetime, timezone, timedelta


def get_source_partitions_condition(
        data_date: str,
        tz_offset: int
) -> str:
    """Get filter condition for selecting source partitions for the given data date."""

    utcdate_base = datetime(
        *(int(p) for p in data_date.split('-')),
        tzinfo=timezone(timedelta(seconds=tz_offset))
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

    return ' OR '.join(
        (
            f"(utcdate = '{utcdate}' AND utchour BETWEEN '{utchours[0]}' AND '{utchours[-1]}')"
        ) for utcdate, utchours in partitions.items()
    )
