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
        the sum total of all credit transaction amounts over the time window
            If there are no credit transactions for a given period, returns zero
    """
    # STEP 1: get a list of all institutions for the user
    institutions = await api_client.institutions.get(user_uuid=user_uuid)

    # subset to only fetch data for institutions known to contain depository-type accounts for the user
    institutions_w_depository = []
    for inst in institutions:
        if "depository" in inst.account_types:
            institutions_w_depository.append(inst)

    # STEP 2: get a list of all transactions for each institution
    inst_coroutines = []
    for inst in institutions_w_depository:
        inst_coroutines.append(
            api_client.transactions.get(
            user_uuid=user_uuid,
            institution_id=inst.institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_time,
            account_types=["depository"],
        )
    )

    transactions_by_institution = await asyncio.gather(*inst_coroutines)

    # STEP 3: now we sum up all the amounts of credit transactions for each institution
    amount = 0
    for transactions in transactions_by_institution:
        for transaction in transactions:
            if transaction.impact == "CREDIT" and transaction.amount is not None:
                amount += transaction.amount

    return amount

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
