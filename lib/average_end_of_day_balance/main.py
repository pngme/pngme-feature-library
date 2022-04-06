#!/usr/bin/env python3

import asyncio
import os
from datetime import datetime, timedelta
from typing import Optional, Tuple

import pandas as pd
from pngme.api import AsyncClient


async def get_average_end_of_day_balance(
    api_client: AsyncClient,
    user_uuid: str,
    utc_time: datetime,
) -> Tuple[Optional[float]]:
    """Calculates the average end-of-day total balance for a user across all
       of their accounts for a given time window.

       Typical date ranges are last 30 days, 31-60 days and 61-90 days.
       If no balance data was found, return None.

    Args:
        api_client: Pngme Async API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_time: the time-zero to use in constructing the 0-30, 31-60 and 61-90 windows

    Returns:
        the average end-of-day total balance for a given time window
    """
    institutions = await api_client.institutions.get(user_uuid=user_uuid)

    # subset to only fetch data for institutions known to contain depository-type accounts for the user
    institutions_w_depository = [
        inst for inst in institutions if "depository" in inst.account_types
    ]

    balance_df_list = []

    # Construct timerange since beginning of time
    # as default /balances endpoint only returns the latest balance
    today = datetime.today()
    all_pages_utc_endtime = datetime(today.year, today.month, today.day)
    all_pages_starttime = all_pages_utc_endtime - timedelta(days=1e5)

    inst_coroutines = [
        api_client.balances.get(
            user_uuid=user_uuid,
            institution_id=institution.institution_id,
            utc_starttime=all_pages_starttime,
            utc_endtime=all_pages_utc_endtime,
            account_types=["depository"],
        )
        for institution in institutions_w_depository
    ]
    r = await asyncio.gather(*inst_coroutines)
    for index, inst_list in enumerate(r):
        depository_balance_records = []
        for bal_rec in inst_list:
            balance_rec_dict = bal_rec.dict()
            depository_balance_records.append(
                {key: balance_rec_dict[key] for key in ["account_id", "ts", "balance"]}
            )
        institution_balances_df = pd.DataFrame(depository_balance_records)
        institution_balances_df = institution_balances_df.assign(
            institution_name=institutions_w_depository[index].institution_id
        )
        balance_df_list.append(institution_balances_df)

    # 1. Combine all institution balances into single dataframe
    balances_df = pd.concat(balance_df_list)
    if balances_df.empty:
        return (None, None, None)

    # 2. Sort and create a column for day, filter by time window
    balances_df = balances_df.sort_values("ts")
    balances_df["yyyymmdd"] = pd.to_datetime(balances_df["ts"], unit="s")
    balances_df["yyyymmdd"] = balances_df["yyyymmdd"].dt.floor("D")

    # 3. Get end of day balances,
    # If an account changes balances three times a day in sequence, i.e. $100, $20, $120),
    # take the last one ($120)
    eod_balances = (
        balances_df.groupby(["yyyymmdd", "account_id", "institution_name"])
        .tail(1)
        .set_index("yyyymmdd")
    )

    # 4. First Forward fill missing days
    ffilled_balances = (
        eod_balances.groupby(["institution_name", "account_id"])["balance"]
        .resample("1D", kind="timestamp")
        .ffill()
        .reset_index()
    )

    # 5. Filter time window for 0-30d, 31-60d ad 61-90d
    # Create time windows for 30d, 60d and 90d
    utc_time_less_30 = utc_time - timedelta(days=30)
    utc_time_less_60 = utc_time - timedelta(days=60)
    utc_time_less_90 = utc_time - timedelta(days=90)

    ffilled_balances_in_time_window_0_30 = ffilled_balances[
        ffilled_balances["yyyymmdd"].between(utc_time_less_30, utc_time)
    ]
    ffilled_balances_in_time_window_31_60 = ffilled_balances[
        ffilled_balances["yyyymmdd"].between(
            utc_time_less_60, utc_time_less_30, "right"
        )
    ]
    ffilled_balances_in_time_window_61_90 = ffilled_balances[
        ffilled_balances["yyyymmdd"].between(
            utc_time_less_90, utc_time_less_60, "right"
        )
    ]

    # 6. Average all balances and calculate a global sum
    avg_daily_balance_0_30 = float(
        ffilled_balances_in_time_window_0_30.groupby(["institution_name", "account_id"])
        .mean()
        .sum()
    )
    avg_daily_balance_31_60 = float(
        ffilled_balances_in_time_window_31_60.groupby(
            ["institution_name", "account_id"]
        )
        .mean()
        .sum()
    )
    avg_daily_balance_61_90 = float(
        ffilled_balances_in_time_window_61_90.groupby(
            ["institution_name", "account_id"]
        )
        .mean()
        .sum()
    )

    return (avg_daily_balance_0_30, avg_daily_balance_31_60, avg_daily_balance_61_90)


if __name__ == "__main__":
    # Mercy Otingo, mercy@pngme.demo, 234112312
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"
    token = os.environ["PNGME_TOKEN"]

    client = AsyncClient(token)

    now = datetime(2021, 10, 1)

    async def main():
        (
            average_end_of_day_balance_0_30,
            average_end_of_day_balance_31_60,
            average_end_of_day_balance_61_90,
        ) = await get_average_end_of_day_balance(
            api_client=client, user_uuid=user_uuid, utc_time=now
        )

        print(average_end_of_day_balance_0_30)
        print(average_end_of_day_balance_31_60)
        print(average_end_of_day_balance_61_90)

    asyncio.run(main())
