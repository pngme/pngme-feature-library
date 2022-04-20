#!/usr/bin/env python3
import asyncio
import os
from datetime import datetime, timedelta

from typing import Tuple
from pngme.api import AsyncClient


async def get_count_loan_defaulted_events(
    api_client: AsyncClient, user_uuid: str, utc_time: datetime
) -> Tuple[int, int, int]:
    """
    Count events labeled with LoanDefaulted across all institutions
    over the following date ranges: last 30 days, 31-60 days and 61-90 days.

    Args:
        api_client: Pngme Async API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_time: the time-zero to use in constructing the 0-30, 31-60 and 61-90 windows

    Returns:
        count of LoanDefaulted events within the given time window
    """
    institutions = await api_client.institutions.get(user_uuid=user_uuid)

    # subset to only fetch data for institutions known to contain loan-type accounts for the user
    institutions_w_loan = [
        inst for inst in institutions if "loan" in inst.account_types
    ]

    # at most, we retrieve data from last 90 days
    utc_starttime = utc_time - timedelta(days=90)
    
    # API call including the label filter so only "LoanDefaulted" events are returned if they exist
    inst_coroutines = [
        api_client.alerts.get(
            user_uuid=user_uuid,
            institution_id=institution.institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_time,
            labels=["LoanDefaulted"],
        )
        for institution in institutions_w_loan
    ]

    r = await asyncio.gather(*inst_coroutines)

    # now we aggregate the results from each institution into a single list
    record_list = []
    for inst_list in r:
        record_list.extend([dict(alert) for alert in inst_list])

    # now we prepare the time windows for the counts
    ts_30 = (utc_time - timedelta(days=30)).timestamp()
    ts_60 = (utc_time - timedelta(days=60)).timestamp()
    ts_90 = (utc_time - timedelta(days=90)).timestamp()

    # now we count the number of events in each time window
    count_loan_defaulted_events_0_30 = 0
    count_loan_defaulted_events_31_60 = 0
    count_loan_defaulted_events_61_90 = 0

    for r in record_list:
        if r["ts"] >= ts_30:
            count_loan_defaulted_events_0_30 += 1
        if r["ts"] >= ts_60 and r["ts"] < ts_30:
            count_loan_defaulted_events_31_60 += 1
        if r["ts"] >= ts_90 and r["ts"] < ts_60:
            count_loan_defaulted_events_61_90 += 1
    
    return (
        count_loan_defaulted_events_0_30,
        count_loan_defaulted_events_31_60,
        count_loan_defaulted_events_61_90,
    )


if __name__ == "__main__":
    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    # Has a LoanDefaulted with toc = 1633712575 (Fri Oct 08 2021 17:02:55 GMT+0000)
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    now = datetime(2021, 12, 1)

    async def main():
        (
            count_loan_defaulted_events_0_30,
            count_loan_defaulted_events_31_60,
            count_loan_defaulted_events_61_90,
        ) = await get_count_loan_defaulted_events(client, user_uuid, now)
        
        print(count_loan_defaulted_events_0_30)
        print(count_loan_defaulted_events_31_60)
        print(count_loan_defaulted_events_61_90)

    asyncio.run(main())
