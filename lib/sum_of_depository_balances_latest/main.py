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

    # STEP 1: get a list of all institutions for the user
    institutions = await api_client.institutions.get(user_uuid=user_uuid)
    utc_starttime = utc_time - cutoff_interval

    # subset to only fetch data for institutions known to contain depository-type accounts for the user
    institutions_w_depository = []
    for inst in institutions:
        if "depository" in inst.account_types:
            institutions_w_depository.append(inst)

    # STEP 2: get a list of all balances for each institution
    inst_coroutines = []
    for inst in institutions_w_depository:
        inst_coroutines.append(
            api_client.balances.get(
                user_uuid=user_uuid,
                institution_id=inst.institution_id,
                utc_starttime=utc_starttime,
                utc_endtime=utc_time,
                account_types=["depository"],
        )
    )

    balances_per_institution = await asyncio.gather(*inst_coroutines)

    # STEP 3: We flatten the lists of balances into a single list of balances
    balances_flattened = []
    for ix, inst_list in enumerate(balances_per_institution):
        institution_id = institutions[ix].institution_id
        for balance in inst_list:
            balances_flattened.append(dict(balance, institution_id=institution_id))
    
    # STEP 5: Here we sort by timestamp so latest balances are on top
    balances_flattened = sorted(balances_flattened, key=lambda x: x["ts"], reverse=True)

    # STEP 6: Then we loop through all balances per institution and account and store the latest balance
    latest_balances = {}
    for loan_record in balances_flattened:
        key = (loan_record["institution_id"], loan_record["account_id"])
        if key not in latest_balances:
            # As we go top-down, we only need to store the first balance we found for each institution+account
            latest_balances[key] = loan_record["balance"]
    
    # STEP 7: Finally, we can sum all the balances
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
