#!/usr/bin/env python3
import asyncio
import os
from datetime import datetime, timedelta

import pandas as pd  # type: ignore
from typing import Tuple

from pngme.api import AsyncClient


async def get_count_insufficient_funds_events(
    api_client: AsyncClient, user_uuid: str, utc_time: datetime
) -> Tuple[int, int, int]:
    """
    Count events labeled with InsufficientFunds across all institutions
    over the following date ranges: last 30 days, 31-60 days and 61-90 days.

    Args:
        api_client: Pngme Async API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_time: the time-zero to use in constructing the 0-30, 31-60 and 61-90 windows

    Returns:
        count of InsufficientFunds events within the given time window
    """
    label = "InsufficientFunds"

    institutions = await api_client.institutions.get(user_uuid=user_uuid)

    # subset to only fetch data for institutions known to contain depository-type accounts for the user
    institutions_w_depository = [
        inst for inst in institutions if "depository" in inst.account_types
    ]

    utc_starttime = utc_time - timedelta(days=90)
    inst_coroutines = [
        api_client.alerts.get(
            user_uuid=user_uuid,
            institution_id=institution.institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_time,
            labels=[label],
        )
        for institution in institutions_w_depository
    ]

    r = await asyncio.gather(*inst_coroutines)

    record_list = []
    for inst_list in r:
        record_list.extend([dict(alert) for alert in inst_list])

    # if no data available for the user, assume count of InsufficientFunds event is zero
    if len(record_list) == 0:
        return 0, 0, 0

    record_df = pd.DataFrame(record_list)

    ts_30 = (utc_time - timedelta(days=30)).timestamp()
    ts_60 = (utc_time - timedelta(days=60)).timestamp()
    ts_90 = (utc_time - timedelta(days=90)).timestamp()

    filter_0_30 = record_df.ts >= ts_30
    filter_31_60 = (record_df.ts >= ts_60) & (record_df.ts < ts_30)
    filter_61_90 = (record_df.ts >= ts_90) & (record_df.ts < ts_60)

    count_insufficient_funds_events_0_30 = len(record_df[filter_0_30])
    count_insufficient_funds_events_31_60 = len(record_df[filter_31_60])
    count_insufficient_funds_events_61_90 = len(record_df[filter_61_90])

    return (
        count_insufficient_funds_events_0_30,
        count_insufficient_funds_events_31_60,
        count_insufficient_funds_events_61_90,
    )


if __name__ == "__main__":
    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    now = datetime(2021, 10, 1)

    async def main():
        (
            count_insufficient_funds_events_0_30,
            count_insufficient_funds_events_31_60,
            count_insufficient_funds_events_61_90,
        ) = await get_count_insufficient_funds_events(client, user_uuid, now)
        print(count_insufficient_funds_events_0_30)
        print(count_insufficient_funds_events_31_60)
        print(count_insufficient_funds_events_61_90)

    asyncio.run(main())
