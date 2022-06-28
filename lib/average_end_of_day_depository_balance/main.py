#!/usr/bin/env python3

import asyncio
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
from pngme.api import AsyncClient


async def get_average_end_of_day_depository_balance(
    api_client: AsyncClient,
    user_uuid: str,
    utc_starttime: datetime,
    utc_endtime: datetime,
) -> Optional[float]:
    """Average of daily of total balance held in all depository accounts.

    Args:
        api_client: Pngme Async API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the UTC time to start the time window
        utc_endtime: the UTC time to end the time window

    Returns:
        If no balance data was found, return None.
    """
    # Making the timestamps timezone aware to comply with the between() method called below
    utc_starttime = utc_starttime.replace(tzinfo=timezone.utc)
    utc_endtime = utc_endtime.replace(tzinfo=timezone.utc)

    institutions = await api_client.institutions.get(user_uuid=user_uuid)

    # subset to only fetch data for institutions known to contain depository-type accounts for the user
    institutions_w_depository = []
    for inst in institutions:
        if "depository" in inst["account_types"]:
            institutions_w_depository.append(inst)

    # We pull an additional 10 days of balance records before the time window because
    # balances are forward filled in time, so this gives us a higher likelihood of
    # beginning the period of interest with valid balance records for each institution
    # rather than containing null values for each institution.
    inst_coroutines = []
    for institution in institutions_w_depository:
        inst_coroutines.append(
            api_client.balances.get(
                user_uuid=user_uuid,
                institution_id=institution["institution_id"],
                utc_starttime=utc_starttime - timedelta(days=10),
                utc_endtime=utc_endtime,
                account_types=["depository"],
            )
        )

    balances_by_institution = await asyncio.gather(*inst_coroutines)

    balances_flattened = []
    for ix, balances in enumerate(balances_by_institution):
        institution_id = institutions_w_depository[ix]["institution_id"]
        # We append the institution_id to each record so that we can group the records
        # by institution_id and account_id
        for balance in balances:
            balance["institution_id"] = institution_id
            balances_flattened.append(balance)

    if len(balances_flattened) == 0:
        return None

    balances_df = pd.DataFrame(balances_flattened)
    balances_df["timestamp"] = pd.to_datetime(balances_df["timestamp"])
    balances_df = balances_df.sort_values("timestamp")
    balances_df["yyyymmdd"] = balances_df["timestamp"].dt.floor("D")

    # Find last balance record on any given day
    eod_balances = (
        balances_df.groupby(["yyyymmdd", "account_id", "institution_id"])
        .tail(1)
        .set_index("yyyymmdd")
    )

    # Carry balance amounts forward in time
    ffilled_balances = (
        eod_balances.groupby(["institution_id", "account_id"])["balance"]
        .resample("1D", kind="timestamp")
        .ffill()
        .reset_index()
    )

    ffilled_balances_in_time_window = ffilled_balances[
        ffilled_balances["yyyymmdd"].between(utc_starttime, utc_endtime)
    ]

    if len(ffilled_balances_in_time_window) == 0:
        return None

    daily_total_balance = ffilled_balances_in_time_window.groupby(["yyyymmdd"])["balance"].sum()
    average_end_of_day_depository_balance = daily_total_balance.mean()
    return average_end_of_day_depository_balance


if __name__ == "__main__":
    # Mercy Otingo, mercy@pngme.demo, 234112312
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"
    token = os.environ["PNGME_TOKEN"]

    client = AsyncClient(token)

    utc_endtime = datetime(2021, 10, 1)
    utc_starttime = utc_endtime - timedelta(days=30)

    async def main():

        average_end_of_day_balance = await get_average_end_of_day_depository_balance(
            api_client=client,
            user_uuid=user_uuid,
            utc_starttime=utc_starttime,
            utc_endtime=utc_endtime,
        )

        print(average_end_of_day_balance)

    asyncio.run(main())
