#!/usr/bin/env python3
import asyncio
import os
from datetime import datetime, timedelta
from re import I

import pandas as pd  # type: ignore

from pngme.api import AsyncClient


async def get_count_betting_and_lottery_events(
    api_client: AsyncClient,
    user_uuid: str,
    utc_starttime: datetime,
    utc_endtime: datetime,
) -> int:
    """
    Count events labeled with BettingAndLottery across all institutions

    Args:
        api_client: Pngme Async API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the UTC time to start the time window
        utc_endtime: the UTC time to end the time window
    Returns:
        count of BettingAndLottery events within the given time window
    """
    institutions = await api_client.institutions.get(user_uuid=user_uuid)
    inst_coroutines = [
        api_client.alerts.get(
            user_uuid=user_uuid,
            institution_id=institution.institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_endtime,
            labels=["BettingAndLottery"],
        )
        for institution in institutions
    ]

    r = await asyncio.gather(*inst_coroutines)

    record_list = []
    for inst_list in r:
        record_list.extend([dict(alert) for alert in inst_list])

    # if no data available for the user, assume count of BettingAndLottery event is zero
    if len(record_list) == 0:
        return 0

    record_df = pd.DataFrame(record_list)

    time_window_filter = (record_df.ts >= utc_starttime.timestamp()) & (
        record_df.ts < utc_endtime.timestamp()
    )

    count_betting_and_lottery_events = len(record_df[time_window_filter])

    return count_betting_and_lottery_events


if __name__ == "__main__":
    # George Hassan, george@pngme.demo.com, 254789012345
    #user_uuid = "6ea9480a-eafa-4eec-a1d2-f1c0c5411ccc"
    user_uuid = "7d96d780-abed-43c8-8f12-4435c9dd8ec5"

    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    utc_endtime = datetime(2021, 10, 1)
    utc_starttime = utc_endtime - timedelta(days=30)

    async def main():

        count_betting_and_lottery_events = await get_count_betting_and_lottery_events(
            client,
            user_uuid,
            utc_starttime,
            utc_endtime,
        )
        print(count_betting_and_lottery_events)

    asyncio.run(main())
