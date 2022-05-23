#!/usr/bin/env python3

import asyncio
import dateutil.parser as date_parser
import os

from datetime import datetime, timedelta, timezone
from pngme.api import AsyncClient
from typing import Optional


async def get_data_recency_minutes(
    api_client: AsyncClient,
    user_uuid: str,
    utc_starttime: datetime,
    utc_endtime: datetime,
) -> Optional[int]:
    """Return the time in minutes between utc_endtime and the most recent financial event or alert,
    as an indicator of data recency, or freshness.
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
                institution_id=inst["institution_id"],
                utc_starttime=utc_starttime,
                utc_endtime=utc_endtime,
                page=1,
            )
        )
        balance_inst_coroutines.append(
            api_client.balances.get(
                user_uuid=user_uuid,
                institution_id=inst["institution_id"],
                utc_starttime=utc_starttime,
                utc_endtime=utc_endtime,
                page=1,
            )
        )
        alerts_inst_coroutines.append(
            api_client.balances.get(
                user_uuid=user_uuid,
                institution_id=inst["institution_id"],
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
    for transactions in transactions_per_institution:
        for transaction in transactions:
            transactions_flattened.append(transaction)

    balances_flattened = []
    for balances in balances_per_institution:
        for balance in balances:
            balances_flattened.append(balance)

    alerts_flattened = []
    for alerts in alerts_per_institution:
        for alert in alerts:
            alerts_flattened.append(alert)

    transactions_sorted = sorted(
        transactions_flattened, key=lambda x: x["timestamp"], reverse=True
    )
    balances_sorted = sorted(
        balances_flattened, key=lambda x: x["timestamp"], reverse=True
    )
    alerts_sorted = sorted(alerts_flattened, key=lambda x: x["timestamp"], reverse=True)

    if not transactions_sorted:
        transactions_ts_recent = 0.0
    else:
        transactions_ts_recent = date_parser.parse(
            transactions_sorted[0]["timestamp"]
        ).timestamp()

    if not balances_sorted:
        balances_ts_recent = 0.0
    else:
        balances_ts_recent = date_parser.parse(
            balances_sorted[0]["timestamp"]
        ).timestamp()

    if not alerts_sorted:
        alerts_ts_recent = 0.0
    else:
        alerts_ts_recent = date_parser.parse(alerts_sorted[0]["timestamp"]).timestamp()

    most_recent_ts = max(transactions_ts_recent, balances_ts_recent, alerts_ts_recent)
    if most_recent_ts == 0.0:
        return None

    data_recency_minutes = round(
        (utc_endtime.replace(tzinfo=timezone.utc).timestamp() - most_recent_ts) / 60
    )
    return data_recency_minutes


if __name__ == "__main__":
    # Mercy Otingo, mercy@pngme.demo, 234112312
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"
    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    utc_endtime = datetime(2021, 11, 1)
    utc_starttime = utc_endtime - timedelta(days=30)

    async def main():
        data_recency_minutes = await get_data_recency_minutes(
            client,
            user_uuid,
            utc_starttime,
            utc_endtime,
        )

        print(data_recency_minutes)

    asyncio.run(main())
