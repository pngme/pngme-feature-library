#!/usr/bin/env python3

import os
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from pngme.api import Client


def get_average_end_of_day_balance(
    client: Client, user_uuid: str, utc_starttime: datetime, utc_endtime: datetime
) -> Optional[float]:
    """Calculates the average end-of-day total balance for a user across all
       of their accounts for a given time window.

       Typical date ranges are last 30 days, 31-60 days and 61-90 days.
       If no balance data was found, return None.

    Args:
        client: Pngme API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the datetime for the left-hand-side of the time-window
        utc_endtime: the datetime for the right-hand-side of the time-window

    Returns:
        the average end-of-day total balance for a given time window
    """
    institutions = client.institutions.get(user_uuid)
    balance_df_list = []

    today = datetime.today()
    all_pages_utc_endtime = datetime(today.year, today.month, today.day)
    all_pages_starttime = all_pages_utc_endtime - timedelta(days=1e5)

    for institution in institutions:
        institution_id = institution.institution_id
        institution_balance_records = client.balances.get(
            user_uuid=user_uuid,
            institution_id=institution_id,
            utc_starttime=all_pages_starttime,
            utc_endtime=all_pages_utc_endtime,
        )
        depository_records = []
        for balance_record in institution_balance_records:
            if balance_record.account_type == "depository":
                balance_record_dict = balance_record.dict()
                depository_records.append(
                    {
                        key: balance_record_dict[key]
                        for key in ["account_number", "ts", "balance"]
                    }
                )
        institution_balances_df = pd.DataFrame(depository_records)
        institution_balances_df = institution_balances_df.assign(
            institution_name=institution_id
        )
        balance_df_list.append(institution_balances_df)

    # 1. Combine all institution balances into single dataframe
    balances_df = pd.concat(balance_df_list)
    if balances_df.empty:
        return None

    # 2. Sort and create a column for day, filter by time window
    balances_df = balances_df.sort_values("ts")
    balances_df["yyyymmdd"] = pd.to_datetime(balances_df["ts"], unit="s")
    balances_df["yyyymmdd"] = balances_df["yyyymmdd"].dt.floor("D")

    # 3. Get end of day balances,
    # If an account changes balances three times a day in sequence, i.e. $100, $20, $120),
    # take the last one ($120)
    eod_balances = (
        balances_df.groupby(["yyyymmdd", "account_number", "institution_name"])
        .tail(1)
        .set_index("yyyymmdd")
    )

    # 4. First Forward fill missing days
    ffilled_balances = (
        eod_balances.groupby(["institution_name", "account_number"])["balance"]
        .resample("1D", kind="timestamp")
        .ffill()
        .reset_index()
    )

    # 5. Filter time window
    ffilled_balances_in_time_window = ffilled_balances[
        ffilled_balances["yyyymmdd"].between(utc_starttime, utc_endtime)
    ]

    # 6. Average all balances and calculate a global sum
    avg_daily_balance = float(
        ffilled_balances_in_time_window.groupby(["institution_name", "account_number"])
        .mean()
        .sum()
    )

    return avg_daily_balance


if __name__ == "__main__":
    # Mercy Otingo, mercy@pngme.demo, 234112312
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"
    token = os.environ["PNGME_TOKEN"]

    client = Client(token)

    now = datetime(2021, 10, 1)
    now_less_30 = now - timedelta(days=30)
    now_less_60 = now - timedelta(days=60)
    now_less_90 = now - timedelta(days=90)

    average_end_of_day_balance_0_30 = get_average_end_of_day_balance(
        client, user_uuid=user_uuid, utc_starttime=now_less_30, utc_endtime=now
    )

    average_end_of_day_balance_31_60 = get_average_end_of_day_balance(
        client, user_uuid=user_uuid, utc_starttime=now_less_60, utc_endtime=now_less_30
    )

    average_end_of_day_balance_61_90 = get_average_end_of_day_balance(
        client, user_uuid=user_uuid, utc_starttime=now_less_90, utc_endtime=now_less_60
    )

    print(average_end_of_day_balance_0_30)
    print(average_end_of_day_balance_31_60)
    print(average_end_of_day_balance_61_90)
