#!/usr/bin/env python3

import asyncio
import os
from datetime import datetime, timedelta
from typing import Optional

from pngme.api import AsyncClient


async def get_sum_of_credits(
    api_client: AsyncClient,
    user_uuid: str,
    utc_starttime: datetime,
    utc_time: datetime,
) -> Optional[float]:
    """
    Sum credit transactions across all depository accounts in a given period.

    No currency conversions are performed.

    Sum of credits is calculated by totaling credit transactions
    across all of a user's depository accounts during the given time period.

    Args:
        api_client: Pngme Async API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the UTC time to start the time window
        utc_endtime: the UTC time to end the time window

    Returns:
        the sum total of all credit transaction amounts over the predefined ranges.
            If there are no credit transactions for a given period, returns None
    """
    institutions = await api_client.institutions.get(user_uuid=user_uuid)

    # subset to only fetch data for institutions known to contain depository-type accounts for the user
    institutions_w_depository = [
        inst for inst in institutions if "depository" in inst.account_types
    ]

    # API call including the account type filter so only "depository" transactions are returned if they exist
    inst_coroutines = [
        api_client.transactions.get(
            user_uuid=user_uuid,
            institution_id=institution.institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_time,
            account_types=["depository"],
        )
        for institution in institutions_w_depository
    ]

    r = await asyncio.gather(*inst_coroutines)

    # now we aggregate the results from each institution into a single list
    record_list = []
    for inst_lst in r:
        record_list.extend([dict(transaction) for transaction in inst_lst])

    # now we sum the transaction amounts in each time window
    amount = None

    for r in record_list:
        if r["impact"] == "CREDIT":
            if r["ts"] >= utc_starttime.timestamp() and r["ts"] < utc_endtime.timestamp():
                amount = _update_total(r["amount"], amount)

    return amount


def _update_total(amount, total):
    if total is None:
        total = 0
    total += amount
    return total


if __name__ == "__main__":
    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    utc_endtime = datetime(2021, 10, 1)
    utc_starttime = utc_endtime - timedelta(days=30)

    async def main():
        soc = await get_sum_of_credits(
            client,
            user_uuid,
            utc_starttime,
            utc_endtime,
        )
        print(soc)

    asyncio.run(main())
