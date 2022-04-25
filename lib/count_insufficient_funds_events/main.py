#!/usr/bin/env python3
import asyncio
import os
from datetime import datetime, timedelta

import pandas as pd  # type: ignore

from pngme.api import AsyncClient


async def get_count_insufficient_funds_events(
    api_client: AsyncClient,
    user_uuid: str,
    utc_starttime: datetime,
    utc_endtime: datetime,
) -> int:
    """
    Count events labeled with InsufficientFunds across all institutions

    Args:
        api_client: Pngme Async API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the UTC time to start the time window
        utc_endtime: the UTC time to end the time window
    Returns:
        count of InsufficientFunds events within the given time window
    """
    institutions = await api_client.institutions.get(user_uuid=user_uuid)

    # subset to only fetch data for institutions known to contain depository-type accounts for the user
    institutions_w_depository = [
        inst for inst in institutions if "depository" in inst.account_types
    ]

    inst_coroutines = [
        api_client.alerts.get(
            user_uuid=user_uuid,
            institution_id=institution.institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_endtime,
            labels=["InsufficientFunds"],
        )
        for institution in institutions_w_depository
    ]

    r = await asyncio.gather(*inst_coroutines)

    record_list = []
    for inst_list in r:
        record_list.extend([dict(alert) for alert in inst_list])

    # if no data available for the user, assume count of InsufficientFunds event is zero
    if len(record_list) == 0:
        return 0

    record_df = pd.DataFrame(record_list)

    time_window_filter = (record_df.ts >= utc_starttime.timestamp()) & (
        record_df.ts < utc_endtime.timestamp()
    )

    count_insufficient_funds_events = len(record_df[time_window_filter])

    return count_insufficient_funds_events


if __name__ == "__main__":
    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    utc_endtime = datetime(2021, 10, 1)
    utc_starttime = utc_endtime - timedelta(days=30)

    async def main():
        count_insufficient_funds_events = await get_count_insufficient_funds_events(
            client,
            user_uuid,
            utc_starttime,
            utc_endtime,
        )
        print(count_insufficient_funds_events)

    asyncio.run(main())
