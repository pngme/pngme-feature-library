#!/usr/bin/env python3
import asyncio
import os

from datetime import datetime, timedelta
from typing import Tuple

import pandas as pd  # type: ignore

from pngme.api import AsyncClient


async def get_average_of_loan_balances(
    api_client: AsyncClient,
    user_uuid: str,
    utc_time: datetime,
) -> Tuple[float, float, float]:
    """Return the average of balances across all loan accounts
    over the following date ranges: 30 days, 31-60 days and 61-90 days from the given utc_time.

    If a loan account does not contain any balance notifications within the time window,
    it is considered stale and not included in the total balance.

    Args:
        api_client: Pngme Async API client
        user_uuid: Pngme mobile phone user_uuid
        utc_time: the time-zero to use in constructing the 0-30, 31-60 and 61-90 windows

    Returns:
        The average of balances across all loan accounts over 3 time-windows
    """

    institutions = await api_client.institutions.get(user_uuid=user_uuid)

    # subset to only fetch data for institutions known to contain loan-type accounts for the user
    institutions_w_loan = [
        inst for inst in institutions if "loan" in inst.account_types
    ]

    utc_starttime = utc_time - timedelta(days=90)
    inst_coroutines = [
        api_client.balances.get(
            user_uuid=user_uuid,
            institution_id=institution.institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_time,
            account_types=["loan"],
        )
        for institution in institutions_w_loan
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
    # consider the average of balances to be zero, if no data is present
    if len(record_list) == 0:
        return 0.0, 0.0, 0.0

    balances_df = pd.DataFrame(record_list)

    ts_30 = (utc_time - timedelta(days=30)).timestamp()
    ts_60 = (utc_time - timedelta(days=60)).timestamp()
    ts_90 = (utc_time - timedelta(days=90)).timestamp()

    filter_0_30 = balances_df.ts >= ts_30
    filter_31_60 = (balances_df.ts >= ts_60) & (balances_df.ts < ts_30)
    filter_61_90 = (balances_df.ts >= ts_90) & (balances_df.ts < ts_60)

    balances_df_0_30 = balances_df[filter_0_30]
    balances_df_31_60 = balances_df[filter_31_60]
    balances_df_61_90 = balances_df[filter_61_90]

    average_of_balances_0_30 = (
        balances_df_0_30.groupby(["institution_id", "account_id"])["balance"]
        .mean()
        .sum()
    )
    average_of_balances_31_60 = (
        balances_df_31_60.groupby(["institution_id", "account_id"])["balance"]
        .mean()
        .sum()
    )
    average_of_balances_61_90 = (
        balances_df_61_90.groupby(["institution_id", "account_id"])["balance"]
        .mean()
        .sum()
    )

    return (
        average_of_balances_0_30,
        average_of_balances_31_60,
        average_of_balances_61_90,
    )


if __name__ == "__main__":
    # Mercy Otingo, mercy@pngme.demo, 234112312
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    now = datetime(2021, 10, 31)

    async def main():
        (
            average_of_balances_0_30,
            average_of_balances_31_60,
            average_of_balances_61_90,
        ) = await get_average_of_loan_balances(client, user_uuid, now)

        print(average_of_balances_0_30)
        print(average_of_balances_31_60)
        print(average_of_balances_61_90)

    asyncio.run(main())
