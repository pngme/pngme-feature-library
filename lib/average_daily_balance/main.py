import os
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from pngme.api import Client


def get_institution_balances(client: Client, user_uuid: str) -> dict:
    """Get all balances for accounts of type 'depository'.
       Returns a dictionary where the key is the insitution_uuid and 
       the value is a dataframe of balances
    Args:
        user_uuid: user identifier
    Returns:
        account_balances: <acct_uuid, balances(pd.DataFrame)>
    """
    institutions = client.institutions.get(user_uuid)
    institution_balances = dict()

    today = datetime.today()
    utc_endtime = datetime(today.year, today.month, today.day)
    utc_starttime = utc_endtime - timedelta(days=1e5)  # get all pages

    for institution in institutions:
        institution_id = institution.institution_id
        balances = client.balances.get(
            user_uuid=user_uuid,
            institution_id=institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_endtime,
        )
        # convert to records
        balances = [
            balance.dict()
            for balance in balances
            if balance.account_type == "depository"
        ]
        balances = pd.DataFrame.from_records(balances)
        institution_balances[institution_id] = balances

    return institution_balances


def _init_eod_sheet(start_ts: int, end_ts: int) -> pd.DataFrame:
    """Get a blank balance sheet
        Initialize a DataFrame with daily frequency and data for columns
        'end_of_day', 'balance' and 'ts'

    Args:
        start_ts: Starting timestamp of the observation window in seconds
        end_ts: Ending timestamp of the observation window in seconds

    Returns: pd.DataFrame
        expected columns:
            - ts: Timestamp
            - end_of_day: Filled with True value
            - balance: Filled with None

    """
    SECONDS_IN_ONE_DAY = 3600 * 24
    days = range(start_ts, end_ts + 1, SECONDS_IN_ONE_DAY)
    eod_sheet = pd.DataFrame(days, columns=["ts"])
    eod_sheet["end_of_day"] = True
    eod_sheet["balance"] = None

    return eod_sheet


def get_eod_balance(
    account_uuid: str, balances: pd.Series, start_ts: int, end_ts: int
) -> pd.Series:
    """Construct the end of day balance sheet

    Args:
        account_uuid
        balance_sheet: DataFrame contains balance information
        start_ts: Starting timestamp of the observation window in seconds
        end_ts: Ending timestamp of the observation window in seconds

    Returns: pd.Series
        End of day account balance sheet
    """
    sod_sheet = balances.copy()
    sod_sheet["end_of_day"] = False

    eod_sheet = _init_eod_sheet(start_ts, end_ts)

    sod_sheet = (
        pd.concat([eod_sheet, sod_sheet], axis=0, ignore_index=True)
        .sort_values("ts")
        .fillna(method="ffill")
    )

    eod_balance = (
        sod_sheet[sod_sheet["end_of_day"] == True][["ts", "balance"]]
        .set_index("ts")
        .rename({"balance": account_uuid}, axis=1)
    )
    return eod_balance


def get_avg_daily_balance(
    account_balances: dict, utc_starttime: datetime, utc_endtime: datetime
) -> float:
    """Compute the daily balance

    Args:
        account_balances: Output from get_user_account_balances
        utc_starttime: the datetime for the left-hand-side of the time-window
        utc_endtime: the datetime for the right-hand-side of the time-window

    Returns: float or None if no data was observed
    """

    start_ts = int((utc_starttime - datetime(1970, 1, 1)).total_seconds())
    end_ts = int((utc_endtime - datetime(1970, 1, 1)).total_seconds())

    eod_balances = []
    for institution_id, balances in account_balances.items():
        if not balances.empty:
            eod_balance = get_eod_balance(institution_id, balances, start_ts, end_ts)
            eod_balances.append(eod_balance)
    return pd.concat(eod_balances, axis=1).sum(axis=1).mean()


if __name__ == "__main__":
    # Mercy Otingo, mercy@pngme.demo, 234112312
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"
    token = os.environ["PNGME_TOKEN"]

    client = Client(token)

    account_balances = get_institution_balances(client=client, user_uuid=user_uuid)

    # avg_daily_balance_0_30
    now = datetime(2021, 10, 1)
    now_less_30 = now - timedelta(days=30)
    now_less_60 = now - timedelta(days=60)
    now_less_90 = now - timedelta(days=90)

    avg_daily_balance_0_30 = get_avg_daily_balance(
        account_balances, utc_starttime=now_less_30, utc_endtime=now
    )

    avg_daily_balance_31_60 = get_avg_daily_balance(
        account_balances, utc_starttime=now_less_60, utc_endtime=now_less_30
    )

    avg_daily_balance_61_90 = get_avg_daily_balance(
        account_balances, utc_starttime=now_less_60, utc_endtime=now_less_90
    )

    print(avg_daily_balance_0_30)
    print(avg_daily_balance_31_60)
    print(avg_daily_balance_61_90)
