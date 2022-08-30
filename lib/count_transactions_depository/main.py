#!/usr/bin/env python3
import asyncio
import os
from datetime import datetime, timedelta

from pngme.api import AsyncClient


async def get_count_transactions_depository(
    api_client: AsyncClient,
    user_uuid: str,
    utc_starttime: datetime,
    utc_endtime: datetime,
) -> int:
    """Gets the count of depository transactions over a given time window.

    Args:
        api_client: Pngme Async API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the UTC time to start the time window
        utc_endtime: the UTC time to end the time window

    Returns:
        count of depository transactions over a given time window
    """
    # STEP 1: fetch list of institutions belonging to the user
    institutions = await api_client.institutions.get(user_uuid=user_uuid)

    institutions_w_depository = []
    for inst in institutions:
        if "depository" in inst["account_types"]:
            institutions_w_depository.append(inst)

    # STEP 2: get all depository transactions for all institutions
    transaction_inst_coroutines = []
    for inst in institutions_w_depository:
        transaction_inst_coroutines.append(
            api_client.transactions.get(
                account_types=["depository"],
                user_uuid=user_uuid,
                institution_id=inst["institution_id"],
                utc_starttime=utc_starttime,
                utc_endtime=utc_endtime,
            )
        )

    # STEP 3: Get the count of all depository transactions
    depository_transactions_count = 0
    transactions_per_institution = await asyncio.gather(*transaction_inst_coroutines)
    for transactions in transactions_per_institution:
        for transaction in transactions:
            if transaction["impact"] == "CREDIT" or transaction["impact"] == "DEBIT":
                depository_transactions_count += 1

    return depository_transactions_count


if __name__ == "__main__":
    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    utc_endtime = datetime(2021, 11, 1)
    utc_starttime = utc_endtime - timedelta(days=30)

    async def main():
        count_transactions_depository = await get_count_transactions_depository(
            client,
            user_uuid,
            utc_starttime,
            utc_endtime,
        )

        print(count_transactions_depository)

    asyncio.run(main())
