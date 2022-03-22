#!/usr/bin/env python3
import asyncio
import os
from datetime import datetime, timedelta

import pandas as pd  # type: ignore
from typing import Tuple

from pngme.api import AsyncClient


async def get_sum_of_balances_latest(
    api_client: AsyncClient, user_uuid: str, utc_time: datetime
) -> Tuple[float, float, float]:
    """Return the latest balance within the time ranges (30d, 60d, 90d) summed across all accounts.

    If an account does not contain any balance notifications within the time window,
    it is considered stale and not included in the total balance.

    Args:
        api_client: Pngme Async API client
        user_uuid: Pngme mobile phone user_uuid
        utc_time: the time-zero to use in constructing the 0-30, 31-60 and 61-90 windows

    Returns:
        The latest balance within the time ranges (30d, 60d, 90d) summed across all accounts
    """

    # Sum the most recent balances from each account within 30d, 60d and 90d windows.
    sum_of_balances_latest_0_30 = 0
    sum_of_balances_latest_31_60 = 0
    sum_of_balances_latest_61_90 = 0

    institutions = await api_client.institutions.get(user_uuid=user_uuid)
    utc_starttime = utc_time - timedelta(days=90)

    inst_coroutines = [
        api_client.balances.get(
            user_uuid=user_uuid,
            institution_id=institution.institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_time,
        )
        for institution in institutions
    ]

    r = await asyncio.gather(*inst_coroutines)

    record_list = []
    for inst_list in r:
        record_list.extend(
            [
                dict(balance)
                for balance in inst_list
                if balance.account_type == "depository"
            ]
        )

    record_df = pd.DataFrame(record_list)

    # Get the total outbound debit over a period
    ts_30 = (utc_time - timedelta(days=30)).timestamp()
    ts_60 = (utc_time - timedelta(days=60)).timestamp()
    ts_90 = (utc_time - timedelta(days=90)).timestamp()

    # Add latest account balance for each account within the institution.
    unique_account_numbers = set(record_df["account_number"].to_list())
    for account_number in unique_account_numbers:
        filter_base = record_df.account_number == account_number

        filter_0_30 = filter_base & (record_df.ts >= ts_30)
        filter_31_60 = filter_base & (record_df.ts >= ts_60) & (record_df.ts < ts_30)
        filter_61_90 = filter_base & (record_df.ts >= ts_90) & (record_df.ts < ts_60)

        sorted_df_0_30 = record_df[filter_0_30].sort_values(by="ts", ascending=False)
        sum_of_balances_latest_0_30 += sorted_df_0_30.iloc[0]["balance"]
        sorted_df_31_60 = record_df[filter_31_60].sort_values(by="ts", ascending=False)
        sum_of_balances_latest_31_60 += sorted_df_31_60.iloc[0]["balance"]
        sorted_df_61_90 = record_df[filter_61_90].sort_values(by="ts", ascending=False)
        sum_of_balances_latest_61_90 += sorted_df_61_90.iloc[0]["balance"]

    return (
        sum_of_balances_latest_0_30,
        sum_of_balances_latest_31_60,
        sum_of_balances_latest_61_90,
    )


if __name__ == "__main__":
    # Mercy Otingo, mercy@pngme.demo, 234112312
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    now = datetime(2021, 10, 31)

    async def main():
        (
            sum_of_balances_latest_0_30,
            sum_of_balances_latest_31_60,
            sum_of_balances_latest_61_90,
        ) = await get_sum_of_balances_latest(client, user_uuid, now)
        print(sum_of_balances_latest_0_30)
        print(sum_of_balances_latest_31_60)
        print(sum_of_balances_latest_61_90)

    asyncio.run(main())
