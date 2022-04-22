#!/usr/bin/env python3
import asyncio
import os
from datetime import datetime, timedelta

from typing import Tuple
from pngme.api import AsyncClient


async def get_count_loan_defaulted_events(
    api_client: AsyncClient,
    user_uuid: str,
    utc_starttime: datetime,
    utc_endtime: datetime,
) -> Tuple[int, int, int]:
    """
    Count events labeled with LoanDefaulted across all institutions
    over the following date ranges: last 30 days, 31-60 days and 61-90 days.

    Args:
        api_client: Pngme Async API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the UTC time to start the time window
        utc_endtime: the UTC time to end the time window

    Returns:
        count of LoanDefaulted events within the given time window
    """
    institutions = await api_client.institutions.get(user_uuid=user_uuid)

    # subset to only fetch data for institutions known to contain loan-type accounts for the user
    institutions_w_loan = [
        inst for inst in institutions if "loan" in inst.account_types
    ]

    # API call including the label filter so only "LoanDefaulted" events are returned if they exist
    inst_coroutines = [
        api_client.alerts.get(
            user_uuid=user_uuid,
            institution_id=institution.institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_endtime,
            labels=["LoanDefaulted"],
        )
        for institution in institutions_w_loan
    ]

    r = await asyncio.gather(*inst_coroutines)

    # now we aggregate the results from each institution into a single list
    record_list = []
    for inst_list in r:
        record_list.extend([dict(alert) for alert in inst_list])

    # now we count the number of events in each time window
    count_loan_defaulted_events = 0

    for r in record_list:
        if r["ts"] >= utc_starttime.timestamp() and r["ts"] < utc_endtime.timestamp():
            count_loan_defaulted_events += 1

    return count_loan_defaulted_events


if __name__ == "__main__":
    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    # Has a LoanDefaulted with toc = 1633712575 (Fri Oct 08 2021 17:02:55 GMT+0000)
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    utc_endtime = datetime(2021, 11, 1)
    utc_starttime = utc_endtime - timedelta(days=30)

    async def main():
        count_loan_defaulted_events = await get_count_loan_defaulted_events(
            client,
            user_uuid,
            utc_starttime,
            utc_endtime,
        )

        print(count_loan_defaulted_events)

    asyncio.run(main())
