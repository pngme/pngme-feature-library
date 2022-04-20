#!/usr/bin/env python3

import asyncio
import os
from datetime import datetime, timedelta
from typing import Tuple

from pngme.api import AsyncClient


async def get_sum_of_debits(
    api_client: AsyncClient, user_uuid: str, utc_time: datetime
) -> Tuple[float, float, float]:
    """Sum debit transactions across all depository accounts in a given period.

    No currency conversions are performed.

    Date ranges are 30 days, 31-60 days and 61-90 days.
    Sum of debits is calculated by totaling all debit transactions
    across all of a user's depository accounts during the given time period.

    Args:
        api_client: Pngme Async API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_time: the time-zero to use in constructing the 0-30, 31-60 and 61-90 windows

    Returns:
        the sum total of all debit transaction amounts over the predefined ranges.
            If there are no debit transactions for a given period, returns None
    """
    institutions = await api_client.institutions.get(user_uuid=user_uuid)

    # subset to only fetch data for institutions known to contain depository-type accounts for the user
    institutions_w_depository = [inst for inst in institutions if "depository" in inst.account_types]

    # at most, we retrieve data from last 90 days
    utc_starttime = utc_time - timedelta(days=90)
    
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
    for inst_list in r:
        record_list.extend([dict(transaction) for transaction in inst_list])

    # Get the total outbound debit over a period
    ts_30 = (utc_time - timedelta(days=30)).timestamp()
    ts_60 = (utc_time - timedelta(days=60)).timestamp()
    ts_90 = (utc_time - timedelta(days=90)).timestamp()

    # now we sum the transaction amounts in each time window
    amount_0_30 = None
    amount_31_60 = None
    amount_61_90 = None

    for r in record_list:
        if r["impact"] == "DEBIT":
            if r["ts"] >= ts_30:
                amount_0_30 = _update_total(r["amount"], amount_0_30)
            elif r["ts"] >= ts_60:
                amount_31_60 = _update_total(r["amount"], amount_31_60)
            elif r["ts"] >= ts_90:
                amount_61_90 = _update_total(r["amount"], amount_61_90)

    return amount_0_30, amount_31_60, amount_61_90

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

    now = datetime(2021, 10, 1)

    async def main():
        soc_0_30, soc_31_60, soc_61_90 = await get_sum_of_debits(client, user_uuid, now)
        print(soc_0_30)
        print(soc_31_60)
        print(soc_61_90)

    asyncio.run(main())
