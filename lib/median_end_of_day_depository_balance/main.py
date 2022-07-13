#!/usr/bin/env python3

import asyncio
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd  # type: ignore
from pngme.api import AsyncClient

# We pull additional days of balance records before the time window because balances are
# forward filled in time, so this gives us a higher likelihood of beginning the period
# of interest with valid balance records for each institution rather than containing
# null values for each institution. We also use this to limit the number of days we
# forward fill a given balance observation to avoid overweighting stale data.
BALANCE_VALID_FOR_DAYS = 10


async def get_median_end_of_day_depository_balance(
    api_client: AsyncClient,
    user_uuid: str,
    utc_starttime: datetime,
    utc_endtime: datetime,
) -> Optional[float]:
    """Median of daily of total balance held in all depository accounts.

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

    balances_coroutines = []
    transactions_coroutines = []
    for institution in institutions_w_depository:
        balances_coroutines.append(
            api_client.balances.get(
                user_uuid=user_uuid,
                institution_id=institution["institution_id"],
                utc_starttime=utc_starttime - timedelta(days=BALANCE_VALID_FOR_DAYS),
                utc_endtime=utc_endtime,
                account_types=["depository"],
            )
        )
        transactions_coroutines.append(
            api_client.transactions.get(
                user_uuid=user_uuid,
                institution_id=institution["institution_id"],
                utc_starttime=utc_starttime,
                utc_endtime=utc_endtime,
            )
        )

    balances_by_institution = await asyncio.gather(*balances_coroutines)
    transactions_by_institution = await asyncio.gather(*transactions_coroutines)

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

    transactions_flattened = []
    for ix, transactions in enumerate(transactions_by_institution):
        for transaction in transactions:
            transactions_flattened.append(transaction)

    transactions_df = pd.DataFrame(transactions_flattened)
    transactions_df["timestamp"] = pd.to_datetime(transactions_df["timestamp"])
    transactions_df = transactions_df.sort_values("timestamp")
    transactions_df["yyyymmdd"] = transactions_df["timestamp"].dt.floor("D")

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
    index = pd.date_range(utc_starttime, utc_endtime, freq="D", name="yyyymmdd")
    ffilled_balances = (
        eod_balances.groupby(["institution_id", "account_id"])["balance"]
        .apply(lambda df: df.reindex(index).ffill(limit=BALANCE_VALID_FOR_DAYS))
        .reset_index()
    )

    ffilled_balances_in_time_window = ffilled_balances[
        ffilled_balances["yyyymmdd"].between(utc_starttime, utc_endtime)
    ]

    if len(ffilled_balances_in_time_window) == 0:
        return None

    daily_total_balance = ffilled_balances_in_time_window.groupby(["yyyymmdd"])[
        "balance"
    ].sum(min_count=1)

    # Include only balances on days the user received a balance or transaction
    active_days = pd.concat(
        [balances_df["yyyymmdd"], transactions_df["yyyymmdd"]]
    ).unique()
    row_mask = daily_total_balance.index.isin(active_days)
    daily_total_balance_on_active_days = daily_total_balance.loc[row_mask]

    return daily_total_balance_on_active_days.median()


if __name__ == "__main__":
    # Mercy Otingo, mercy@pngme.demo, 234112312
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"
    token = os.environ["PNGME_TOKEN"]

    client = AsyncClient(token)

    utc_endtime = datetime(2021, 10, 1)
    utc_starttime = utc_endtime - timedelta(days=30)

    async def main():

        median_end_of_day_depository_balance = (
            await get_median_end_of_day_depository_balance(
                api_client=client,
                user_uuid=user_uuid,
                utc_starttime=utc_starttime,
                utc_endtime=utc_endtime,
            )
        )

        print(median_end_of_day_depository_balance)

    asyncio.run(main())
