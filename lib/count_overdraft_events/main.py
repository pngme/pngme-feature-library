#!/usr/bin/env python3
import asyncio
import os
from datetime import datetime, timedelta, timezone

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
    # Make sure the timestamps are of UTC timezone
    utc_starttime = utc_starttime.astimezone(timezone.utc).replace(tzinfo=None)
    utc_endtime = utc_endtime.astimezone(timezone.utc).replace(tzinfo=None)

    # STEP 1: fetch list of institutions belonging to the user
    institutions = await api_client.institutions.get(user_uuid=user_uuid)

    # STEP 2: fetch alert records for all institutions with Overdraft events
    alerts_coroutines = []
    for inst in institutions:
        alerts_coroutines.append(
            api_client.alerts.get(
                user_uuid=user_uuid,
                institution_id=inst["institution_id"],
                utc_starttime=utc_starttime,
                utc_endtime=utc_endtime,
                labels=["Overdraft"],
            )
        )

    alerts_by_institution = await asyncio.gather(*alerts_coroutines)

    # STEP 3: flatten alerts into a single list
    all_alerts = []
    for alerts_list in alerts_by_institution:
        for alert in alerts_list:
            all_alerts.append(alert)

    # STEP 4: count number of alerts
    return len(all_alerts)


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
