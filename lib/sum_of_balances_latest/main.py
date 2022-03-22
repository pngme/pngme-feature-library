#!/usr/bin/env python3
import asyncio
import os
from datetime import datetime, timedelta

import pandas as pd  # type: ignore
from typing import Dict, Tuple

from pngme.api import AsyncClient


async def get_sum_of_balances_latest(
    api_client: AsyncClient,
    user_uuid: str,
    utc_time: datetime,
    cutoff_interval: timedelta = timedelta(days=90),
) -> float:
    """Return the latest balance summed across all accounts.

    If an account does not contain any balance notifications within the time window,
    it is considered stale and not included in the total balance.

    Args:
        api_client: Pngme Async API client
        user_uuid: Pngme mobile phone user_uuid
        utc_time: the time at which the latest balance is computed
        cutoff_interval: if balance hasn't been updated within this interval, then balance record is stale, and method returns 0

    Returns:
        The latest balance summed across all accounts
    """

    # Sum the most recent balances from each account.
    sum_of_balances_latest = 0

    institutions = await api_client.institutions.get(user_uuid=user_uuid)
    utc_starttime = utc_time - cutoff_interval

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
    institution_ids = [institution.institution_id for institution in institutions]

    balances_by_institution_id = dict(zip(institution_ids, r))

    depository_balance_df_by_institution_id: Dict[str, pd.DataFrame] = {}
    for institution_id, balances in balances_by_institution_id.items():

        depository_balances = [
            balance for balance in balances if balance.account_type == "depository"
        ]

        if depository_balances:
            depository_balance_df_by_institution_id[institution_id] = pd.DataFrame(
                [balance.dict() for balance in depository_balances]
            )

    # Add latest account balance for each account within the institution.
    for institution_id, balances_df in depository_balance_df_by_institution_id.items():
        unique_account_numbers = set(balances_df["account_number"].to_list())
        for account_number in unique_account_numbers:
            filter_account_number = balances_df.account_number == account_number

            sorted_df_by_timestamp = balances_df[filter_account_number].sort_values(
                by="ts", ascending=False
            )
            sum_of_balances_latest += sorted_df_by_timestamp.iloc[0]["balance"]

    return sum_of_balances_latest


if __name__ == "__main__":
    # Mercy Otingo, mercy@pngme.demo, 234112312
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    now = datetime(2021, 10, 31)

    async def main():
        sum_of_balances_latest = await get_sum_of_balances_latest(
            client, user_uuid, now
        )
        print(sum_of_balances_latest)

    asyncio.run(main())
