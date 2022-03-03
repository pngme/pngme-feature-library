#!/usr/bin/env python3
"""
Sums of credits calculate the total inbound cash over a time period
for a given user over all their depository accounts.
The sum of inbound cash is a proxy for a client's income.
Typical date ranges are last 30 days, 31-60 days and 61-90 days.
"""
from datetime import datetime
from datetime import timedelta
from pngme.api import Client

import os
import pandas as pd


def sum_of_credits(
    api_client: Client, user_uuid: str, utc_starttime: datetime, utc_endtime: datetime
) -> float:
    """Calculates the sum of credit-type transactions for a given user across all
     depository accounts in a given period. No currency conversions are performed.

    Args:
        api_client: Pngme API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the datetime for the left-hand-side of the time-window
        utc_endtime: the datetime for the right-hand-side of the time-window

    Returns:
        the sum total of all credit transaction amounts
    """
    institutions = api_client.institutions.get(user_uuid=user_uuid)

    # Constructs a dataframe that contains transactions from all institutions for the user
    record_list = []
    for institution in institutions:
        transactions = api_client.transactions.get(
            user_uuid=user_uuid,
            institution_id=institution.institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_endtime,
        )
        record_list.extend([dict(transaction) for transaction in transactions])

    # if no data available for the user, assume cash-in is zero
    if len(record_list) == 0:
        return 0.0

    record_df = pd.DataFrame(record_list)

    # Get the total inbound credit over a period
    amount = record_df[
        (record_df.impact == "CREDIT")  # Subset by credit
        & (
            record_df.account_type.isin(["depository"])
        )  # Only consider depository account
    ].amount.sum()
    return amount


if __name__ == "__main__":

    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    USER_UUID = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    client = Client(access_token=os.environ["PNGME_TOKEN"])

    now = datetime(2021, 10, 1)
    now_less_30 = now - timedelta(days=30)
    now_less_60 = now - timedelta(days=60)
    now_less_90 = now - timedelta(days=90)

    sum_of_credits_0_30 = sum_of_credits(client, USER_UUID, now_less_30, now)
    sum_of_credits_31_60 = sum_of_credits(client, USER_UUID, now_less_60, now_less_30)
    sum_of_credits_61_90 = sum_of_credits(client, USER_UUID, now_less_90, now_less_60)
