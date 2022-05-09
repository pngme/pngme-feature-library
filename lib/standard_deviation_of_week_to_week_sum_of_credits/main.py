#!/usr/bin/env python3

import asyncio
import os
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd  # type: ignore
from pngme.api import AsyncClient


async def get_standard_deviation_of_week_to_week_sum_of_credits(
    client: AsyncClient, user_uuid: str, utc_starttime: datetime, utc_endtime: datetime
) -> Optional[float]:
    """Compute the standard deviations of week-to-week sum of credits

    Args:
        client: Pngme API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the datetime for the left-hand-side of the time-window
        utc_endtime: the datetime for the right-hand-side of the time-window

    Returns:
        Standard deviation of week-to-week sum of credits
    """
    # STEP 1: fetch list of institutions belonging to the user
    institutions = await client.institutions.get(user_uuid=user_uuid)

    # subset to only fetch data for institutions known to contain depository-type accounts for the user
    institutions_w_depository = []
    for inst in institutions:
        if "depository" in inst.account_types:
            institutions_w_depository.append(inst)

    # Constructs a dataframe that contains transactions from all institutions for the user
    coroutines = []
    for institution in institutions_w_depository:
        coroutines.append(
            client.transactions.get(
                user_uuid=user_uuid,
                institution_id=institution.institution_id,
                utc_starttime=utc_starttime,
                utc_endtime=utc_endtime,
                account_types=["depository"],
            )
        )

    transactions_by_institution = await asyncio.gather(*coroutines)

    # if no data available for the user, return None
    if len(transactions_by_institution) == 0:
        return None

    record_list_flattened = []
    for transactions in transactions_by_institution:
        for transaction in transactions:
            # We cast as dictionary so we can generate a dataframe easily
            record_list_flattened.append(dict(transaction))

    record_df = pd.DataFrame(record_list_flattened)
    credit_df = record_df[(record_df.impact == "CREDIT")]

    # if no data available for credit, return None
    if credit_df.empty:
        return None

    credit_df.insert(0, "date", pd.to_datetime(credit_df.ts, unit="s"))
    std = credit_df.set_index("date").resample("W").amount.sum().std()
    return std


if __name__ == "__main__":
    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    now = datetime(2021, 11, 1)
    now_less_84 = now - timedelta(days=84)

    async def main():
        standard_deviation_of_week_to_week_sum_of_credits_0_84 = (
            await get_standard_deviation_of_week_to_week_sum_of_credits(
                client=client,
                user_uuid=user_uuid,
                utc_starttime=now_less_84,
                utc_endtime=now,
            )
        )
        

        print(standard_deviation_of_week_to_week_sum_of_credits_0_84)

    asyncio.run(main())
