#!/usr/bin/env python3
"""
Sums of credits calculate the total inbound cash over a time period for a given user over all their accounts.
The sum of inbound cash is a proxy for a client's income.
Typical date ranges are last 30 days, 31-60 days and 61-90 days.
"""
from typing import Optional

import pendulum
import pandas as pd
import requests


API_TOKEN = "MY_API_TOKEN"  # paste token here to run script

SECONDS_IN_ONE_DAY = 3600 * 24
HEADER = {
    "Accept": "application/json",
    "Authorization": "Bearer {api_token}".format(api_token=API_TOKEN),
}
ACCOUNT_BASE_URL = "https://api.pngme.com/beta/users/{user_uuid}/accounts"
TRANSACTION_BASE_URL = "https://api.pngme.com/beta/users/{user_uuid}/accounts/{acct_uuid}/transactions?utc_starttime=1970-01-01T00%3A00%3A00"


def _get_transactions(user_uuid: str, acct_uuid: str) -> dict:
    """Get transaction for a given user and account
    Args:
        user_uuid: user UUID
        acct_uuid: account UUID
    Returns (dict):
        JSON object with transactions
    """

    transaction_url = TRANSACTION_BASE_URL.format(
        user_uuid=user_uuid, acct_uuid=acct_uuid
    )
    return requests.request("GET", transaction_url, headers=HEADER).json()


def _construct_user_transactions(
    user_uuid: str, account_details: dict
) -> Optional[pd.DataFrame]:
    """Constructs a dataframe that contains transactions from all accounts for a given user.
    Args:
        user_uuid: User UUID
        account_details: Account details in account API response
    Returns:
        Record data frame if there is any else None
    """
    record_list = []
    for individual_account in account_details["accounts"]:
        transaction_response = _get_transactions(
            user_uuid=user_uuid, acct_uuid=individual_account["acct_uuid"]
        )
        for entry in transaction_response.get("transactions"):
            record_list.extend(entry.get("records", []))
    account_record_df = pd.DataFrame(record_list)
    return account_record_df if account_record_df.empty else None


def _get_total_inbound_credit(
    user_record_df: pd.DataFrame, epoch_start: int, epoch_end: int
) -> float:
    """Get the total inbound credit over a period
    Args:
        user_record_df (Optional<pd.DataFrame>): Record DataFrame
            expected columns:
                - impact (str)
                - ts (int)
                - amount (float)
                - account_type (str)
        epoch_start: the UTC epoch timestamp for the left-hand-side of the time-window
        epoch_end: Ending timestamp of the observation window in seconds
    Returns:
        Total inbound amount in the time window
    """
    if user_record_df is None:
        return 0.0

    return user_record_df[
        (user_record_df.impact == "CREDIT")  # Subset by credit
        & (
            user_record_df.ts.between(epoch_start, epoch_end, inclusive="right")
        )  # Apply observation window
        # Exclude loan type inbound amount
        & (
            ~user_record_df.account_type.isin(["loan", "revolving_loan"])
        )  # Filter out loan related transactions
    ].amount.sum()


def sum_of_credits(user_uuid: str, epoch_start: int, epoch_end: int) -> float:
    """Calculates the sum of credit-type transactions for a given user across all depository accounts in a given period.
    No currency conversions are performed.

    Args:
        user_uuid: the Pngme user_uuid for the mobile phone user
        epoch_start: the UTC epoch timestamp for the left-hand-side of the time-window
        epoch_end: the UTC epoch timestamp for the left-hand-side of the time-window

    Returns:
        the sum total of all credit transaction amounts
    """
    account_details = requests.request(
        "GET", ACCOUNT_BASE_URL.format(user_uuid=user_uuid), headers=HEADER
    ).json()
    record_df = _construct_user_transactions(
        user_uuid=USER_UUID, account_details=account_details
    )
    return _get_total_inbound_credit(
        user_record_df=record_df, epoch_start=epoch_start, epoch_end=epoch_end
    )


if __name__ == "__main__":

    USER_UUID = "c9f0624d-4e7a-41cc-964d-9ea3b268427f"  # user: (Moses Ali, moses@pngme.demo.com, 254678901234)

    now = pendulum.now()
    now_less_30 = now.subtract(days=30)
    now_less_60 = now.subtract(days=60)
    now_less_90 = now.subtract(days=90)

    sum_of_credits_0_30 = sum_of_credits(
        USER_UUID, now_less_30.int_timestamp, now.int_timestamp
    )
    sum_of_credits_31_60 = sum_of_credits(
        USER_UUID, now_less_60.int_timestamp, now_less_60.int_timestamp
    )
    sum_of_credits_61_90 = sum_of_credits(
        USER_UUID, now_less_90.int_timestamp, now_less_90.int_timestamp
    )

    df = pd.DataFrame(
        {
            "user_uuid": USER_UUID,
            "sum_of_credits_0_30": sum_of_credits_0_30,
            "sum_of_credits_31_60": sum_of_credits_31_60,
            "sum_of_credits_61_90": sum_of_credits_61_90,
        }
    )

    print(df.to_markdown())
