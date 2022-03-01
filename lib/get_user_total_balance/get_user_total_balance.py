#!/usr/bin/env python3
"""
Sums all account balances for a user at a given time
"""
from datetime import timedelta
from typing import Optional
import os
import time
from pngme.api import Client
import pandas as pd


def get_account_balance_at_timestamp(
    user_balance_df: pd.DataFrame,
    timestamp: int,
    balance_ts_col: str = "ts",
    account_number_col: str = "account_number",
    bal_col: str = "balance",
    warning_days: int = 30,
) -> Optional[float]:
    """Get the total balance of a user at specific timestamp

    Args:
        user_balance_df: User balance dataframe
        timestamp: timestamp for the total balance of interest
        balance_ts_col: Column in user_balance_df indicates the balance timestamp column
        account_number_col: Column in user_balance_df indicates the account_number column
        bal_col: Column in user_balance_df indicates the balance column
        warning_days: Maximal days that balance info will be considered as stale

    Returns:
        Total balance if exists otherwise None
    """
    if user_balance_df.empty:
        print(
            f"Warning: Do not have balance data prior to timestamp {timestamp}."
            f" Will consider it as 0."
        )
        return None
    account_number = user_balance_df.iloc[0][account_number_col]

    _balance_df = user_balance_df[user_balance_df[balance_ts_col] <= timestamp]
    if _balance_df.empty:
        print(
            f"Warning: Do not have balance data for {account_number}"
            f" prior to timestamp {timestamp}. Will consider it as 0."
        )
        return None

    latest_balance_entry = _balance_df.sort_values(balance_ts_col).iloc[-1]
    if (timestamp - latest_balance_entry[balance_ts_col]) >= timedelta(
        days=warning_days
    ).total_seconds():
        print(
            f"Warning: Balance data for {account_number} at timestamp {timestamp}"
            f" is stale more than {warning_days} days."
        )

    return latest_balance_entry[bal_col]


def get_total_balance(api_client: Client, user_uuid: str, timestamp: int) -> float:
    """Get user total balance at a specific timestamp
    Will output 0 coupled with warning if no balance data was available

    Args:
        api_client: Pngme API client
        user_uuid: User UUID string
        timestamp: Timestamp of interest for total user balance

    Returns:
        User total balance at a specific time
    """
    total_balance = 0.0

    accounts = api_client.accounts.get(user_uuid=user_uuid)
    # Loop through accounts for balance data
    for account in accounts:
        account_balances = api_client.balances.get(
            user_uuid=user_uuid, account_uuid=account.acct_uuid
        )
        for account_balance in account_balances:
            account_balance_df = pd.DataFrame(
                [
                    dict(account_balance_entry)
                    for account_balance_entry in account_balance
                ]
            )
            balance_result = get_account_balance_at_timestamp(
                user_balance_df=account_balance_df, timestamp=timestamp
            )
            total_balance += balance_result if balance_result is not None else 0

    return total_balance


if __name__ == "__main__":

    USER_UUID = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    client = Client(access_token=os.environ["PNGME_TOKEN"])

    decision_time = int(time.time())
    decision_time_30_days_ago = decision_time - int(timedelta(days=30).total_seconds())
    get_total_balance_30_gao = get_total_balance(
        api_client=client, user_uuid=USER_UUID, timestamp=decision_time_30_days_ago
    )
