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
    utc_starttime: datetime,
    utc_endtime: datetime,
) -> Tuple[Optional[float]]:
    """Sum observed minimum balances across all depository accounts over a given time window.

    Args:
        api_client: Pngme Async API client
        user_uuid: Pngme mobile phone user_uuid
        utc_starttime: the UTC time to start the time window
        utc_endtime: the UTC time to end the time window

    Returns:
        Sum of minimum balance observed for all depository accounts, in a given time window.
        If balance information was not observed, it returns None.
    """

    institutions = await api_client.institutions.get(user_uuid=user_uuid)

    # subset to only fetch data for institutions known to contain depository-type accounts for the user
    institutions_w_depository = [
        inst for inst in institutions if "depository" in inst.account_types
    ]

    inst_coroutines = [
        api_client.balances.get(
            user_uuid=user_uuid,
            institution_id=institution.institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_endtime,
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

    time_window_filter = (balances_df.ts >= utc_starttime.timestamp()) & (
        balances_df.ts < utc_endtime.timestamp()
    )

    sum_of_account_minimum_balances = (
        balances_df[time_window_filter]
        .groupby(["institution_id", "account_id"])["balance"]
        .min()
        .sum()
    )

    return sum_of_account_minimum_balances


if __name__ == "__main__":
    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    utc_endtime = datetime(2021, 10, 1)
    utc_starttime = utc_endtime - timedelta(days=30)

    async def main():
        sum_of_minimum_balances = await get_sum_of_minimum_balances(
            api_client=client,
            user_uuid=user_uuid,
            utc_starttime=utc_starttime,
            utc_endtime=utc_endtime,
        )

        print(sum_of_minimum_balances)

    asyncio.run(main())
