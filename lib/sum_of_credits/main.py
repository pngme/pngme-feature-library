#!/usr/bin/env python3

import asyncio
import os
from datetime import datetime, timedelta
import pandas as pd  # type: ignore
from typing import Tuple

from pngme.api import AsyncClient


async def get_sum_of_credits(api_client: AsyncClient, user_uuid: str, utc_time: datetime) -> Tuple[float, float, float]:
    """
    Sum credit transactions across all depository accounts in a given period.

    No currency conversions are performed.

    Date ranges are 30 days, 31-60 days and 61-90 days.
    Sum of credits is calculated by totaling credit transactions
    across all of a user's depository accounts during the given time period.

    Args:
        api_client: Pngme Async API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_time: the time-zero to use in constructing the 0-30, 31-60 and 61-90 windows

    Returns:
        the sum total of all credit transaction amounts over the predefined ranges.
    """
    institutions = await api_client.institutions.get(user_uuid=user_uuid)

    # Constructs a dataframe that contains transactions from all institutions for the user
    record_list = []
    utc_starttime = utc_time - timedelta(days=90)
    inst_coroutines = [
        api_client.transactions.get(user_uuid=user_uuid,
                                    institution_id=institution.institution_id,
                                    utc_starttime=utc_starttime,
                                    utc_endtime=utc_time)
        for institution in institutions]
    r = await asyncio.gather(*inst_coroutines)
    for inst_lst in r:
        record_list.extend([dict(transaction) for transaction in inst_lst])

    # if no data available for the user, assume cash-in is zero
    if len(record_list) == 0:
        return 0.0, 0.0, 0.0

    record_df = pd.DataFrame(record_list)

    # Get the total inbound credit over a period
    ts_30 = (utc_time - timedelta(days=30)).timestamp()
    ts_60 = (utc_time - timedelta(days=60)).timestamp()
    ts_90 = (utc_time - timedelta(days=90)).timestamp()

    filter_base = (record_df.impact == "CREDIT") & (record_df.account_type == "depository")
    filter_0_30 = filter_base & (record_df.ts >= ts_30)
    filter_31_60 = filter_base & (record_df.ts >= ts_60) & (record_df.ts < ts_30)
    filter_61_90 = filter_base & (record_df.ts >= ts_90) & (record_df.ts < ts_60)

    amount_0_30 = record_df[filter_0_30].amount.sum()
    amount_31_60 = record_df[filter_31_60].amount.sum()
    amount_61_90 = record_df[filter_61_90].amount.sum()

    return amount_0_30, amount_31_60, amount_61_90


if __name__ == "__main__":
    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    now = datetime(2021, 10, 1)
    ts = datetime.now()

    async def main():
        soc_0_30, soc_31_60, soc_61_90 = await get_sum_of_credits(client, user_uuid, now)
        print(soc_0_30)
        print(soc_31_60)
        print(soc_61_90)

    asyncio.run(main())
