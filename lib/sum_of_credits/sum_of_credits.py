#!/usr/bin/env python3
"""
Sums of credits calculate the total inbound cash over a time period
for a given user over all their accounts.
The sum of inbound cash is a proxy for a client's income.
Typical date ranges are last 30 days, 31-60 days and 61-90 days.
"""
from datetime import datetime
from datetime import timedelta
from pngme.api import Client

import pandas as pd


def sum_of_credits(
    api_client: Client, user_uuid: str, epoch_start: datetime, epoch_end: datetime
) -> float:
    """Calculates the sum of credit-type transactions for a given user across all
     depository accounts in a given period. No currency conversions are performed.

    Args:
        api_client: Pngme API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        epoch_start: the datetime for the left-hand-side of the time-window
        epoch_end: the datetime for the right-hand-side of the time-window

    Returns:
        the sum total of all credit transaction amounts
    """
    account_details = api_client.accounts.get(user_uuid=user_uuid)

    # Constructs a dataframe that contains transactions from all accounts for the user
    record_list = []
    for individual_account in account_details:
        transaction_response = api_client.transactions.get(
            user_uuid=user_uuid,
            account_uuid=individual_account.acct_uuid,
            utc_starttime=epoch_start,
            utc_endtime=epoch_end,
        )
        record_list.extend([dict(entry) for entry in transaction_response])
    record_df = pd.DataFrame(record_list)

    if record_df is None:
        return 0.0

    # Get the total inbound credit over a period
    return record_df[
        (record_df.impact == "CREDIT")  # Subset by credit
        & (
            ~record_df.account_type.isin(["loan", "revolving_loan", "mobile-money"])
        )  # Filter out loan and mobile money account
    ].amount.sum()


if __name__ == "__main__":

    API_TOKEN = "MY_API_TOKEN"  # paste token here to run script
    USER_UUID = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    client = Client(access_token=API_TOKEN)

    now = datetime(2022, 3, 1)
    now_less_30 = now - timedelta(days=30)
    now_less_60 = now - timedelta(days=60)
    now_less_90 = now - timedelta(days=90)

    sum_of_credits_0_30 = sum_of_credits(client, USER_UUID, now_less_30, now)
    sum_of_credits_31_60 = sum_of_credits(client, USER_UUID, now_less_60, now_less_30)
    sum_of_credits_61_90 = sum_of_credits(client, USER_UUID, now_less_90, now_less_60)
