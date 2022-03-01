#!/usr/bin/env python3
"""
Sums of credits calculate the total inbound cash over a time period
for a given user over all their accounts.
The sum of inbound cash is a proxy for a client's income.
Typical date ranges are last 30 days, 31-60 days and 61-90 days.
"""
from pngme.api import Client

import pendulum
import pandas as pd


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

    # Constructs a dataframe that contains transactions from all accounts for the user
    record_list = []
    for individual_account in account_details:
        transaction_response = api_client.transactions.get(
            user_uuid=user_uuid, account_uuid=individual_account.acct_uuid
        )
        record_list.extend([dict(entry) for entry in transaction_response])
    record_df = pd.DataFrame(record_list)

    if record_df is None:
        return 0.0

    # Get the total inbound credit over a period
    return record_df[
        (record_df.impact == "CREDIT")  # Subset by credit
        & (
            record_df.ts.between(epoch_start, epoch_end, inclusive="right")
        )  # Apply observation window
        # Exclude loan type inbound amount
        & (
            ~record_df.account_type.isin(["loan", "revolving_loan", "mobile-money"])
        )  # Filter out loan and mobile money account
    ].amount.sum()


if __name__ == "__main__":

    API_TOKEN = "MY_API_TOKEN"  # paste token here to run script
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
