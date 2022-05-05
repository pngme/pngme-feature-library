#!/usr/bin/env python3
import asyncio
import os
from datetime import datetime, timedelta

from pngme.api import AsyncClient


async def get_sum_of_loan_balances_latest(
    api_client: AsyncClient,
    user_uuid: str,
    utc_starttime: datetime,
    utc_endtime: datetime,
) -> float:
    """Return the latest balance summed across all loan accounts.

    If a loan account does not contain any balance notifications within the time window,
    it is considered stale and not included in the total balance.

    Args:
        api_client: Pngme Async API client
        user_uuid: Pngme mobile phone user_uuid
        utc_starttime: the time from which balances are considered
        utc_endtime: the time until which balances are considered

    Returns:
        The latest balance summed across all loan accounts
    """
    
    # STEP 1: get a list of all institutions for the user
    institutions = await api_client.institutions.get(user_uuid=user_uuid)

    # subset to only fetch data for institutions known to contain loan-type accounts for the user
    institutions_w_loan = []
    for inst in institutions:
        if "loan" in inst.account_types:
            institutions_w_loan.append(inst)

    # STEP 2: get a list of all balances for each institution
    inst_coroutines = []
    for inst in institutions_w_loan:
        inst_coroutines.append(
            api_client.balances.get(
                user_uuid=user_uuid,
                institution_id=inst.institution_id,
                utc_starttime=utc_starttime,
                utc_endtime=utc_endtime,
                account_types=["loan"],
        )
    )

    balances_per_institution = await asyncio.gather(*inst_coroutines)

    # STEP 3: We flatten the lists of balances into a single list of balances
    balances_flattened = []
    for ix, balances in enumerate(balances_per_institution):
        institution_id = institutions[ix].institution_id
        for balance in balances:
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
    cutoff_interval = timedelta(days=30)
    utc_starttime = now - cutoff_interval

    async def main():
        sum_of_balances_latest = await get_sum_of_loan_balances_latest(
            api_client=client,
            user_uuid=user_uuid,
            utc_starttime=utc_starttime,
            utc_endtime=now,
        )
        print(sum_of_balances_latest)
    asyncio.run(main())
