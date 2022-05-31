#!/usr/bin/env python3
import asyncio
import os
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

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
    # STEP 1: get a list of all institutions for the user
    institutions = await api_client.institutions.get(user_uuid=user_uuid)

    # subset to only fetch data for institutions known to contain depository-type accounts for the user
    institutions_w_depository = []
    for inst in institutions:
        if "depository" in inst["account_types"]:
            institutions_w_depository.append(inst)

    # STEP 2: get a list of all balances for each institution
    inst_coroutines = []
    for inst in institutions_w_depository:
        inst_coroutines.append(
            api_client.balances.get(
                user_uuid=user_uuid,
                institution_id=inst["institution_id"],
                utc_starttime=utc_starttime,
                utc_endtime=utc_endtime,
                account_types=["depository"],
            )
        )

    balances_by_institution = await asyncio.gather(*inst_coroutines)
    balances_flattened = []
    for ix, balances in enumerate(balances_by_institution):
        institution_id = institutions[ix]["institution_id"]
        # We append the institution_id to each record so that we can
        # group the records by institution_id and account_id
        for balance in balances:
            balance_dict = dict(balance)
            balance_dict["institution_id"] = institution_id

            balances_flattened.append(balance_dict)

    # We only care about the minimum balance observed for each account of each institution
    min_balances = {}
    for record in balances_flattened:
        key = (record["institution_id"], record["account_id"])
        if key not in min_balances:
            min_balances[key] = record["balance"]
        else:
            min_balances[key] = min(min_balances[key], record["balance"])

    sum_of_account_minimum_balances = sum(min_balances.values())

    return sum_of_account_minimum_balances


if __name__ == "__main__":
    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    utc_endtime = datetime(2021, 11, 1)
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
