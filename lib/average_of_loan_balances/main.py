#!/usr/bin/env python3
import asyncio
import os

from datetime import datetime, timedelta
from typing import Optional

import pandas as pd  # type: ignore

from pngme.api import AsyncClient


async def get_average_of_loan_balances(
    api_client: AsyncClient,
    user_uuid: str,
    utc_starttime: datetime,
    utc_endtime: datetime,
) -> Optional[float]:
    """Return the average of balances across all loan accounts
    over the following date ranges: 30 days, 31-60 days and 61-90 days from the given utc_time.

    If a loan account does not contain any balance notifications within the time window,
    it is considered stale and not included in the total balance.

    Args:
        api_client: Pngme Async API client
        user_uuid: Pngme mobile phone user_uuid
        utc_starttime: the UTC time to start the time window
        utc_endtime: the UTC time to end the time window

    Returns:
        The average of balances across all loan accounts over 3 time-windows
    """

    institutions = await api_client.institutions.get(user_uuid=user_uuid)

    # subset to only fetch data for institutions known to contain loan-type accounts for the user
    institutions_w_loan = [
        inst for inst in institutions if "loan" in inst.account_types
    ]

    inst_coroutines = [
        api_client.balances.get(
            user_uuid=user_uuid,
            institution_id=institution.institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_endtime,
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

    if len(record_list) == 0:
        return None

    balances_df = pd.DataFrame(record_list)

    time_window_filter = (balances_df.ts >= utc_starttime.timestamp()) & (
        balances_df.ts <= utc_endtime.timestamp()
    )

    balances_df = balances_df[time_window_filter]

    average_of_balances = (
        balances_df.groupby(["institution_id", "account_id"])["balance"].mean().sum()
    )

    return average_of_balances


if __name__ == "__main__":
    # Mercy Otingo, mercy@pngme.demo, 234112312
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    utc_endtime = datetime(2021, 10, 1)
    utc_starttime = utc_endtime - timedelta(days=30)

    async def main():

        average_of_balances = await get_average_of_loan_balances(
            api_client=client,
            user_uuid=user_uuid,
            utc_starttime=utc_starttime,
            utc_endtime=utc_endtime,
        )
        print(average_of_balances)

    asyncio.run(main())
