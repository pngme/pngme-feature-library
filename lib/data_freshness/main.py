#!/usr/bin/env python3

import asyncio
import os
from datetime import datetime, timedelta
from pngme.api import AsyncClient


async def get_data_freshness(
    api_client: AsyncClient,
    user_uuid: str,
    utc_starttime: datetime,
    utc_endtime: datetime,
    page: int,
) -> float:
    """Return the time in minutes between utc_endtime and the most recent financial event or alert,
    as an indicator of data freshness.
    Args:
        api_client: Pngme Async API client
        user_uuid: Pngme mobile phone user_uuid
        utc_starttime: the time from which balances are considered
        utc_endtime: the time until which balances are considered
        page: the page(s) requested
    Returns:
        Return the time in minutes between utc_endtime and the most recent financial event or alert
    """

    # STEP 1: get lists of transactions, alerts, and balances for each institution
    institutions = await api_client.institutions.get(user_uuid=user_uuid)

    # STEP a2: get a list of all transactions for each institution
    transaction_inst_coroutine = []
    for inst in institutions:
        transaction_inst_coroutine.append(
            api_client.transactions.get(
                user_uuid=user_uuid,
                institution_id=inst.institution_id,
                utc_starttime=utc_starttime,
                utc_endtime=utc_endtime,
                page=1,
            )
        )

    transactions_per_institution = await asyncio.gather(*transaction_inst_coroutine)

    # STEP a3: flatten all balances from all institutions
    # These do not emerge as sorted, likely due to the by-institution sorting. (must check about pagination).
    transactions_flattened = []
    for ix, transactions in enumerate(transactions_per_institution):
        institution_id = institutions[ix].institution_id
        for transaction in transactions:
            transactions_flattened.append(
                dict(transaction, institution_id=institution_id)
            )

    # STEP a4: Here we sort by timestamp so latest transactions are on top and keep the first entry
    transactions_flattened = sorted(
        transactions_flattened, key=lambda x: x["ts"], reverse=True
    )

    # STEP a5: Get timestamp of first transaction
    if transactions_flattened is None:
        transactions_max_ts = 0
    else:
        transactions_max_ts = transactions_flattened[0]["ts"]


    # STEP b2: get a list of all balances for each institution
    balance_inst_coroutines = []
    for inst in institutions:
        balance_inst_coroutines.append(
            api_client.balances.get(
                user_uuid=user_uuid,
                institution_id=inst.institution_id,
                utc_starttime=utc_starttime,
                utc_endtime=utc_endtime,
                page=1,
            )
        )

    balances_per_institution = await asyncio.gather(*balance_inst_coroutines)

    # STEP b3: flatten all balances from all institutions
    balances_flattened = []
    for ix, balances in enumerate(balances_per_institution):
        institution_id = institutions[ix].institution_id
        for balance in balances:
            balances_flattened.append(dict(balance, institution_id=institution_id))

    # STEP b4: Here we sort by timestamp so latest transactions are on top and keep the first entry
    balances_flattened = sorted(balances_flattened, key=lambda x: x["ts"], reverse=True)

    # STEP b5: Get timestamp of first transaction
    if balances_flattened is None:
        balances_max_ts = 0
    else:
        balances_max_ts = balances_flattened[0]["ts"]


    # STEP c2: get a list of all alerts for each institution
    alerts_inst_coroutines = []
    for inst in institutions:
        alerts_inst_coroutines.append(
            api_client.balances.get(
                user_uuid=user_uuid,
                institution_id=inst.institution_id,
                utc_starttime=utc_starttime,
                utc_endtime=utc_endtime,
                page=1,
            )
        )

    alerts_per_institution = await asyncio.gather(*alerts_inst_coroutines)

    # STEP c3: flatten all balances from all institutions
    alerts_flattened = []
    for ix, alerts in enumerate(alerts_per_institution):
        institution_id = institutions[ix].institution_id
        for alert in alerts:
            alerts_flattened.append(dict(alert, institution_id=institution_id))

    # STEP c4: Here we sort by timestamp so latest transactions are on top and keep the first entry
    alerts_flattened = sorted(alerts_flattened, key=lambda x: x["ts"], reverse=True)

    # STEP c5: Get timestamp of first transaction
    if alerts_flattened is None:
        alerts_max_ts = 0
    else:
        alerts_max_ts = alerts_flattened[0]["ts"]


    # STEP 6: Finally, we can get the minimum of the data freshness (in days)
    most_recent_ts = max(transactions_max_ts, balances_max_ts, alerts_max_ts)
    if most_recent_ts is None:
        data_freshness = None
    else:
        data_freshness = (utc_endtime.timestamp() - most_recent_ts)/60

    return data_freshness


if __name__ == "__main__":
    # Mercy Otingo, mercy@pngme.demo, 234112312
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"
    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    utc_endtime = datetime(2021, 11, 1)
    utc_starttime = utc_endtime - timedelta(days=30)
    page = 1

    async def main():
        data_freshness = await get_data_freshness(
            client,
            user_uuid,
            utc_starttime,
            utc_endtime,
            page,
        )

        print(data_freshness)

    asyncio.run(main())
