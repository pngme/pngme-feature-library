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
) -> int:
    """
    Count events labeled with LoanDefaulted across all institutions
    over the range between utc_starttime and utc_endtime.

    Args:
        api_client: Pngme Async API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the UTC time to start the time window
        utc_endtime: the UTC time to end the time window

    Returns:
        count of LoanDefaulted events within the given time window
    """

    # STEP 1: fetch a list of the user's institutions with loan-type data
    institutions = await api_client.institutions.get(user_uuid=user_uuid)
    institutions_w_loan = []
    for inst in institutions:
        if "loan" in inst["account_types"]:
            institutions_w_loan.append(inst)

    # STEP 2: fetch alert records containing the LoanDefaulted label, for each institution with loan-type data
    inst_coroutines = []
    for inst in institutions_w_loan:
        inst_coroutines.append(
            api_client.alerts.get(
                user_uuid=user_uuid,
                institution_id=inst["institution_id"],
                utc_starttime=utc_starttime,
                utc_endtime=utc_endtime,
                labels=["LoanDefaulted"],
            )
        )

    # async fetch alerts across institutions with loans
    alerts_by_institution = await asyncio.gather(*inst_coroutines)

    # STEP 3: count the total number of LoanDefaulted alert records across all institutions with loan data
    count_loan_defaulted_events = 0
    for alerts in alerts_by_institution:
        count_loan_defaulted_events += len(alerts)

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
