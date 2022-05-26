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
    """Calculates the average end-of-day depository balance total for a user across all
       of their accounts for a given time window.

       If no balance data was found, return None.

    Args:
        api_client: Pngme Async API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the UTC time to start the time window
        utc_endtime: the UTC time to end the time window

    Returns:
        the average end-of-day total balance over each window
    """
    # STEP 0: Make sure the timestamps are of UTC timezone
    utc_starttime = utc_starttime.astimezone(timezone.utc)
    utc_endtime = utc_endtime.astimezone(timezone.utc)

    # STEP 1: fetch list of institutions belonging to the user
    institutions = await api_client.institutions.get(user_uuid=user_uuid)

    # subset to only fetch data for institutions known to contain depository-type accounts for the user
    institutions_w_depository = []
    for inst in institutions:
        if "depository" in inst["account_types"]:
            institutions_w_depository.append(inst)

    # STEP 2: Construct timerange since beginning of time
    # as default /balances endpoint only returns the latest balance
    today = datetime.today()
    all_pages_utc_endtime = datetime(today.year, today.month, today.day)
    all_pages_starttime = all_pages_utc_endtime - timedelta(days=1e5)

    # STEP 3: fetch all balances for each institution using parallel calls
    inst_coroutines = []
    for institution in institutions_w_depository:
        inst_coroutines.append(
            api_client.balances.get(
                user_uuid=user_uuid,
                institution_id=institution["institution_id"],
                utc_starttime=all_pages_starttime,
                utc_endtime=all_pages_utc_endtime,
                account_types=["depository"],
            )
        )

    balances_by_institution = await asyncio.gather(*inst_coroutines)

    # STEP 4: flatten all balances from all institutions
    record_list = []
    for ix, balances in enumerate(balances_by_institution):
        institution_id = institutions[ix]["institution_id"]
        for balance in balances:
            balance_dict = dict(balance)
            balance_dict["institution_id"] = institution_id

            record_list.append(balance_dict)

    # if no data is present, consider the sum of balances to be non-existing
    if len(record_list) == 0:
        return None

    # STEP 5: convert to pandas dataframe to help with the resampling
    balances_df = pd.DataFrame(record_list)

    # Sort and create a column for day, filter by time window
    balances_df["timestamp"] = pd.to_datetime(balances_df["timestamp"])
    balances_df = balances_df.sort_values("timestamp")
    balances_df["yyyymmdd"] = balances_df["timestamp"].dt.floor("D")

    # Get end of day balances,
    # If an account changes balances three times a day in sequence, i.e. $100, $20, $120),
    # take the last one ($120)
    eod_balances = (
        balances_df.groupby(["yyyymmdd", "account_id", "institution_id"])
        .tail(1)
        .set_index("yyyymmdd")
    )

    # First Forward fill missing days
    ffilled_balances = (
        eod_balances.groupby(["institution_id", "account_id"])["balance"]
        .resample("1D", kind="timestamp")
        .ffill()
        .reset_index()
    )

    # Filter time window for the provided period
    ffilled_balances_in_time_window = ffilled_balances[
        ffilled_balances["yyyymmdd"].between(utc_starttime, utc_endtime)
    ]

    # Average all balances and calculate a global sum
    avg_daily_balance = float(
        ffilled_balances_in_time_window.groupby(["institution_id", "account_id"])
        .mean()
        .sum()
    )

    return avg_daily_balance


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
