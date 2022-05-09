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

    # STEP 1: fetch list of institutions belonging to the user
    institutions = await api_client.institutions.get(user_uuid=user_uuid)

    # STEP 2: get a list of all transactions for each institution
    transaction_inst_coroutine = []
    balance_inst_coroutines = []
    alerts_inst_coroutines = []
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
        balance_inst_coroutines.append(
            api_client.balances.get(
                user_uuid=user_uuid,
                institution_id=inst.institution_id,
                utc_starttime=utc_starttime,
                utc_endtime=utc_endtime,
                page=1,
            )
        )
        alerts_inst_coroutines.append(
            api_client.balances.get(
                user_uuid=user_uuid,
                institution_id=inst.institution_id,
                utc_starttime=utc_starttime,
                utc_endtime=utc_endtime,
                page=1,
            )
        )

    transactions_per_institution = await asyncio.gather(*transaction_inst_coroutine)
    balances_per_institution = await asyncio.gather(*balance_inst_coroutines)
    alerts_per_institution = await asyncio.gather(*alerts_inst_coroutines)


    # STEP 3: flatten all balances, transactions, and alerts from all institutions, and get most recent time
    transactions_flattened = []
    for ix, transactions in enumerate(transactions_per_institution):
        institution_id = institutions[ix].institution_id
        for transaction in transactions:
            transactions_flattened.append(
                dict(transaction, institution_id=institution_id))

    balances_flattened = []
    for ix, balances in enumerate(balances_per_institution):
        institution_id = institutions[ix].institution_id
        for balance in balances:
            balances_flattened.append(dict(balance, institution_id=institution_id))
    
    alerts_flattened = []
    for ix, alerts in enumerate(alerts_per_institution):
        institution_id = institutions[ix].institution_id
        for alert in alerts:
            alerts_flattened.append(dict(alert, institution_id=institution_id))


    transactions_sorted = sorted(transactions_flattened, key=lambda x: x["ts"], reverse=True)
    balances_sorted = sorted(balances_flattened, key=lambda x: x["ts"], reverse=True)
    alerts_sorted = sorted(alerts_flattened, key=lambda x: x["ts"], reverse=True)


    if not transactions_sorted:
        transactions_ts_recent = 0
    else:
        transactions_ts_recent = transactions_sorted[0]["ts"]
        
    if not balances_sorted:
        balances_ts_recent = 0
    else:
        balances_ts_recent = balances_sorted[0]["ts"]

    if not alerts_sorted:
        alerts_ts_recent = 0
    else:
        alerts_ts_recent = alerts_sorted[0]["ts"]

    most_recent_ts = max(transactions_ts_recent, balances_ts_recent, alerts_ts_recent)
    if most_recent_ts is None:
        data_freshness = None
    else:
        data_freshness = round((utc_endtime.timestamp() - most_recent_ts) / 60)

    return data_freshness


if __name__ == "__main__":
    # Mercy Otingo, mercy@pngme.demo, 234112312
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"
    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    utc_endtime = datetime(2021, 11, 1)
    utc_starttime = utc_endtime - timedelta(days=30)

    async def main():
        data_freshness = await get_data_freshness(
            client,
            user_uuid,
            utc_starttime,
            utc_endtime,
        )

        print(data_freshness)

    asyncio.run(main())
