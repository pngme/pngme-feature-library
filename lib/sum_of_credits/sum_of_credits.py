#!/usr/bin/env python3
"""
Sums of credits calculate the total inbound cash over a time period
for a given user over all their accounts.
The sum of inbound cash is a proxy for a client's income.
Typical date ranges are last 30 days, 31-60 days and 61-90 days.
"""
from typing import List
from typing import Optional
from pngme.api import Client
from pngme.api import Account

import pendulum
import pandas as pd


API_TOKEN = "MY_API_TOKEN"  # paste token here to run script

SECONDS_IN_ONE_DAY = 3600 * 24


def _construct_user_transactions(
    api_client: Client, user_uuid: str, account_details: List[Account]
) -> Optional[pd.DataFrame]:
    """Constructs a dataframe that contains transactions from all accounts for a given user.
    Args:
        api_client: Pngme API client
        user_uuid: User UUID
        account_details: Account details in account API response
    Returns:
        Record data frame if there is any else None
    """
    record_list = []
    for individual_account in account_details:
        transaction_response = api_client.transactions.get(
            user_uuid=user_uuid, account_uuid=individual_account.acct_uuid
        )
        record_list.extend([dict(entry) for entry in transaction_response])
    account_record_df = pd.DataFrame(record_list)
    return account_record_df if not account_record_df.empty else None


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


def sum_of_credits(
    api_client: Client, user_uuid: str, epoch_start: int, epoch_end: int
) -> float:
    """Calculates the sum of credit-type transactions for a given user across all
     depository accounts in a given period. No currency conversions are performed.

    Args:
        api_client: Pngme API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        epoch_start: the UTC epoch timestamp for the left-hand-side of the time-window
        epoch_end: the UTC epoch timestamp for the left-hand-side of the time-window

    Returns:
        the sum total of all credit transaction amounts
    """
    account_details = api_client.accounts.get(user_uuid=user_uuid)
    record_df = _construct_user_transactions(
        api_client=api_client, user_uuid=USER_UUID, account_details=account_details
    )
    return _get_total_inbound_credit(
        user_record_df=record_df, epoch_start=epoch_start, epoch_end=epoch_end
    )


if __name__ == "__main__":

    USER_UUID = "c9f0624d-4e7a-41cc-964d-9ea3b268427f"
    client = Client(access_token=API_TOKEN)

    now = pendulum.now()
    now_less_30 = now.subtract(days=30)
    now_less_60 = now.subtract(days=60)
    now_less_90 = now.subtract(days=90)

    sum_of_credits_0_30 = sum_of_credits(
        client, USER_UUID, now_less_30.int_timestamp, now.int_timestamp
    )
    sum_of_credits_31_60 = sum_of_credits(
        client, USER_UUID, now_less_60.int_timestamp, now_less_60.int_timestamp
    )
    sum_of_credits_61_90 = sum_of_credits(
        client, USER_UUID, now_less_90.int_timestamp, now_less_90.int_timestamp
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
