#!/usr/bin/env python3
import asyncio
import os
from datetime import datetime, timedelta
from typing import Optional, Tuple
import pandas as pd  # type: ignore

from pngme.api import AsyncClient


async def get_sum_of_minimum_balances(
    api_client: AsyncClient,
    user_uuid: str,
    utc_time: datetime,
    cutoff_interval: timedelta = timedelta(days=90),
) -> Tuple[Optional[float]]:
    """Sum observed minimum balances across all depository accounts over a given time window.

    Args:
        api_client: Pngme Async API client
        user_uuid: Pngme mobile phone user_uuid
        utc_time: the time at which the balances are computed
        cutoff_interval: if balance hasn't been updated within this interval, then balance record is stale, and method returns None

    Returns:
        Sum of minimum balance observed for all depository accounts, in a given time window.
        If balance information was not observed, it returns None.
    """

    institutions = await api_client.institutions.get(user_uuid=user_uuid)
    utc_starttime = utc_time - cutoff_interval

    # subset to only fetch data for institutions known to contain depository-type accounts for the user
    institutions_w_depository = [
        inst for inst in institutions if "depository" in inst.account_types
    ]

    inst_coroutines = [
        api_client.balances.get(
            user_uuid=user_uuid,
            institution_id=institution.institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_time,
            account_types=["depository"],
        )
        for institution in institutions_w_depository
    ]

    r = await asyncio.gather(*inst_coroutines)
    record_list = []
    for ix, inst_list in enumerate(r):
        institution_id = institutions[ix].institution_id
        record_list.extend(
            [
                dict(transaction, institution_id=institution_id)
                for transaction in inst_list
            ]
        )

    # return None, if no data is present
    if len(record_list) == 0:
        return (None, None, None)

    balances_df = pd.DataFrame(record_list)

    ts_30 = (utc_time - timedelta(days=30)).timestamp()
    ts_60 = (utc_time - timedelta(days=60)).timestamp()
    ts_90 = (utc_time - timedelta(days=90)).timestamp()

    filter_0_30 = balances_df.ts >= ts_30
    filter_31_60 = (balances_df.ts >= ts_60) & (balances_df.ts < ts_30)
    filter_61_90 = (balances_df.ts >= ts_90) & (balances_df.ts < ts_60)

    sum_of_account_minimum_balances_0_30 = (
        balances_df[filter_0_30]
        .groupby(["institution_id", "account_id"])["balance"]
        .min()
        .sum()
    )
    sum_of_account_minimum_balances_31_60 = (
        balances_df[filter_31_60]
        .groupby(["institution_id", "account_id"])["balance"]
        .min()
        .sum()
    )
    sum_of_account_minimum_balances_61_90 = (
        balances_df[filter_61_90]
        .groupby(["institution_id", "account_id"])["balance"]
        .min()
        .sum()
    )

    return (
        sum_of_account_minimum_balances_0_30,
        sum_of_account_minimum_balances_31_60,
        sum_of_account_minimum_balances_61_90,
    )


if __name__ == "__main__":
    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    decision_time = datetime(2021, 10, 1)

    async def main():
        (
            sum_of_minimum_balances_0_30,
            sum_of_minimum_balances_31_60,
            sum_of_minimum_balances_61_90,
        ) = await get_sum_of_minimum_balances(
            api_client=client,
            user_uuid=user_uuid,
            utc_time=decision_time,
        )

        print(sum_of_minimum_balances_0_30)
        print(sum_of_minimum_balances_31_60)
        print(sum_of_minimum_balances_61_90)

    asyncio.run(main())
