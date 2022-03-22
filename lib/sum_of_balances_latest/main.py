#!/usr/bin/env python3
import asyncio
import os
from datetime import datetime, timedelta
import pandas as pd  # type: ignore

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

    institutions = await api_client.institutions.get(user_uuid=user_uuid)
    utc_starttime = utc_time - cutoff_interval

    inst_coroutines = [
        api_client.balances.get(
            user_uuid=user_uuid,
            institution_id=institution.institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_time,
            account_types=['depository'],
        )
        for institution in institutions
    ]

    r = await asyncio.gather(*inst_coroutines)
    record_list = []
    for ix, inst_list in enumerate(r):
        institution_id = institutions[ix].institution_id
        record_list.extend([dict(transaction, institution_id=institution_id) for transaction in inst_list])
    # consider the sum of balances to be zero, if no data is present
    if len(record_list) == 0:
        return 0.0

    balances_df = pd.DataFrame(record_list)
    # sort df desc so we can take the top values as latest ts within each group
    balances_df.sort_values(by=['ts'], ascending=False, inplace=True)
    sum_of_balances_latest = balances_df.groupby(["institution_id", "account_number"]).head(1)['balance'].sum()
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
