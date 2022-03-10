import os
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from pngme.api import Client


def get_avg_daily_balance(
    client: Client, user_uuid: str, utc_starttime: datetime, utc_endtime: datetime
) -> float:
    """Calculates the average daily account balance for a user across all
       of their accounts for a given time window. 

       Typical date ranges are last 30 days, 31-60 days and 61-90 days.
       
    Args:
        client: Pngme API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the datetime for the left-hand-side of the time-window
        utc_endtime: the datetime for the right-hand-side of the time-window
    Returns:
        the average daily account balance for a given time window
    """
    institutions = client.institutions.get(user_uuid)
    institution_balances = []

    today = datetime.today()
    all_pages_utc_endtime = datetime(today.year, today.month, today.day)
    all_pages_starttime = all_pages_utc_endtime - timedelta(days=1e5)  # get all pages

    # 0. Get all balances for a user
    for institution in institutions:
        institution_id = institution.institution_id
        balances = client.balances.get(
            user_uuid=user_uuid,
            institution_id=institution_id,
            utc_starttime=all_pages_starttime,
            utc_endtime=all_pages_utc_endtime,
        )
        # convert to records
        balances = [
            balance.dict()
            for balance in balances
            if (balance.account_type == "depository")
        ]
        balances = pd.DataFrame.from_records(balances)
        balances = balances.assign(institution_name=institution_id)
        institution_balances.append(balances)

    # 1. Combine all institution balances into single dataframe
    institution_balances_df = pd.concat(institution_balances)
    if institution_balances_df.empty:
        return None

    # 2. Sort and create a column for day, filter by time window
    institution_balances_df = institution_balances_df.sort_values("ts")
    institution_balances_df["yyyymmdd"] = pd.to_datetime(institution_balances_df['ts'], unit='s')
    institution_balances_df["yyyymmdd"] = institution_balances_df["yyyymmdd"].dt.floor("D")

    # 3. Filter time window
    institution_balances_df = institution_balances_df[
        (institution_balances_df["yyyymmdd"] >= utc_starttime)
        & (institution_balances_df["yyyymmdd"] <= utc_endtime)
    ]
    if institution_balances_df.empty:
        return None

    # 4. Get end of day balances, if an account changes balances three times a day ($100, $20, $120), take the last one ($120)
    eod_balances = institution_balances_df.groupby(
        ["yyyymmdd", "account_number", "institution_name"]
    ).tail(1)
    eod_balances = eod_balances.set_index("yyyymmdd")

    # 5. Forward fill missing days. Average all balances and calculate a global sum
    ffilled_balances = (
        eod_balances.groupby(["institution_name", "account_number"])["balance"]
        .resample("1D", kind="timestamp")
        .ffill()
    )
    avg_daily_balance = (
        ffilled_balances.groupby(["institution_name", "account_number"]).mean().sum()
    )

    return avg_daily_balance


if __name__ == "__main__":
    # Mercy Otingo, mercy@pngme.demo, 234112312
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"
    token = os.environ["PNGME_TOKEN"]

    client = Client(token)

    # avg_daily_balance_0_30
    now = datetime(2021, 10, 1)
    now_less_30 = now - timedelta(days=30)
    now_less_60 = now - timedelta(days=60)
    now_less_90 = now - timedelta(days=90)

    avg_daily_balance_0_30 = get_avg_daily_balance(
        client, user_uuid=user_uuid, utc_starttime=now_less_30, utc_endtime=now
    )

    avg_daily_balance_31_60 = get_avg_daily_balance(
        client, user_uuid=user_uuid, utc_starttime=now_less_60, utc_endtime=now_less_30
    )

    avg_daily_balance_61_90 = get_avg_daily_balance(
        client, user_uuid=user_uuid, utc_starttime=now_less_90, utc_endtime=now_less_60
    )

    print(avg_daily_balance_0_30)
    print(avg_daily_balance_31_60)
    print(avg_daily_balance_61_90)
