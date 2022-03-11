#!/usr/bin/env python3

import os
from datetime import datetime, timedelta

import pandas as pd  # type: ignore
from pngme.api import Client


def get_sum_of_debits(
    api_client: Client, user_uuid: str, utc_starttime: datetime, utc_endtime: datetime
) -> float:
    """Sum debit transactions across all depository accounts in a given period.

    No currency conversions are performed. Typical date ranges are last 30 days, 31-60
    days and 61-90 days. Sum of debits is calculated by totaling debit transactions
    across all of a user's depository accounts during the given time period.

    Args:
        api_client: Pngme API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the datetime for the left-hand-side of the time-window
        utc_endtime: the datetime for the right-hand-side of the time-window

    Returns:
        the sum total of all debit transaction amounts
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

    # if no data available for the user, assume cash-out is zero
    if len(record_list) == 0:
        return 0.0

    record_df = pd.DataFrame(record_list)

    # Get the total cash-outs (debits) over a period
    amount = record_df[
        (record_df.impact == "DEBIT") & (record_df.account_type == "depository")
    ].amount.sum()
    return amount


if __name__ == "__main__":
    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = Client(token)

    now = datetime(2021, 10, 1)
    now_less_30 = now - timedelta(days=30)
    now_less_60 = now - timedelta(days=60)
    now_less_90 = now - timedelta(days=90)

    sum_of_debits_0_30 = get_sum_of_debits(
        api_client=client,
        user_uuid=user_uuid,
        utc_starttime=now_less_30,
        utc_endtime=now,
    )
    sum_of_debits_31_60 = get_sum_of_debits(
        api_client=client,
        user_uuid=user_uuid,
        utc_starttime=now_less_60,
        utc_endtime=now_less_30,
    )
    sum_of_debits_61_90 = get_sum_of_debits(
        api_client=client,
        user_uuid=user_uuid,
        utc_starttime=now_less_90,
        utc_endtime=now_less_60,
    )

    print(sum_of_debits_0_30)
    print(sum_of_debits_31_60)
    print(sum_of_debits_61_90)
