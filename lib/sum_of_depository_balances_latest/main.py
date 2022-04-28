#!/usr/bin/env python3
import asyncio
import os
from datetime import datetime, timedelta

from pngme.api import AsyncClient


async def get_sum_of_depository_balances_latest(
    api_client: AsyncClient,
    user_uuid: str,
    utc_time: datetime,
    cutoff_interval: timedelta = timedelta(days=90),
) -> float:
    """Return the latest balance summed across all depository accounts.

    If a depository account does not contain any balance notifications within the time window,
    it is considered stale and not included in the total balance.

    Args:
        api_client: Pngme Async API client
        user_uuid: Pngme mobile phone user_uuid
        utc_time: the time at which the latest balance is computed
        cutoff_interval: if balance hasn't been updated within this interval, then balance record is stale, and method returns 0

    Returns:
        The latest balance summed across all depository accounts
    """

    institutions = await api_client.institutions.get(user_uuid=user_uuid)
    utc_starttime = utc_time - cutoff_interval

    # subset to only fetch data for institutions known to contain depository-type accounts for the user
    institutions_w_depository = [inst for inst in institutions if "depository" in inst.account_types]

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
    records_list = []
    for ix, inst_list in enumerate(r):
        institution_id = institutions[ix].institution_id
        records_list.extend([dict(transaction, institution_id=institution_id) for transaction in inst_list])
    
    # Here we sort by timestamp so latest balances are on top
    records_list = sorted(records_list, key=lambda x: x["ts"], reverse=True)

    # Then we loop through all balances per institution and account and store the latest balance
    latest_balances = {}
    for loan_record in records_list:
        key = (loan_record["institution_id"], loan_record["account_id"])
        if key not in latest_balances:
            latest_balances[key] = loan_record["balance"]
    
    # Finally, we can sum all the balances
    sum_of_balances_latest = sum(latest_balances.values())

    return sum_of_balances_latest


if __name__ == "__main__":
    # Mercy Otingo, mercy@pngme.demo, 234112312
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    now = datetime(2021, 10, 31)

    async def main():
        sum_of_balances_latest = await get_sum_of_depository_balances_latest(
            client, user_uuid, now
        )
        print(sum_of_balances_latest)
    asyncio.run(main())
