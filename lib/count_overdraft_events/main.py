#!/usr/bin/env python3
import asyncio
import os
from datetime import datetime, timedelta

from pngme.api import AsyncClient


async def get_count_overdraft_events(
    api_client: AsyncClient,
    user_uuid: str,
    utc_starttime: datetime,
    utc_endtime: datetime,
) -> int:
    """
    Count events labeled with Overdraft across all institutions

    Args:
        api_client: Pngme Async API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the UTC time to start the time window
        utc_endtime: the UTC time to end the time window

    Returns:
        count of Overdraft events within the given time window
    """
    institutions = await api_client.institutions.get(user_uuid=user_uuid)

    inst_coroutines = [
        api_client.alerts.get(
            user_uuid=user_uuid,
            institution_id=institution.institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_endtime,
            labels=["Overdraft"],
        )
        for institution in institutions
    ]

    r = await asyncio.gather(*inst_coroutines)

    record_list = []
    for inst_list in r:
        record_list.extend([dict(alert) for alert in inst_list])

    count_overdraft_events = 0
    for alert in record_list:
        if alert["ts"] >= utc_starttime.timestamp() and alert["ts"] < utc_endtime.timestamp():
            count_overdraft_events += 1
    
    return count_overdraft_events


if __name__ == "__main__":
    # Segun Sani, segun@pngme.demo.com, 2346789012345
    user_uuid = "88719687-ec14-4cda-8262-c5da67228a67"

    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    utc_endtime = datetime(2021, 10, 1)
    utc_starttime = utc_endtime - timedelta(days=30)

    async def main():
        count_overdraft_events = await get_count_overdraft_events(
            client,
            user_uuid,
            utc_starttime,
            utc_endtime,
        )

        print(count_overdraft_events)

    asyncio.run(main())
