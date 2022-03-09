#!/usr/bin/env python3

import os
from datetime import datetime, timedelta

import pandas as pd
from pngme.api import Client


def get_debt_to_income_ratio(
    api_client: Client, user_uuid: str, utc_starttime: datetime, utc_endtime: datetime
) -> float:
    """Compute the debt to income ratio over a period

    Debt: Sum of open, late_payment and default tradeline balance
    Income: Sum credit transactions across all depository accounts in a given period.

    Args:
        api_client: Pngme API client
        user_uuid: Pngme mobile phone user_uuid
        utc_starttime: the datetime for the left-hand-side of the time-window
        utc_endtime: the datetime for the right-hand-side of the time-window

    Return:
        Ratio between debt to income ratio.
        If debt amount is zero, return 0
        If income is zero, return float('inf')
    """

    credit_report = api_client.credit_report.get(user_uuid)

    # Sum the values of all open, late_payment and default tradelines as debt proxy.
    tradelines = [
        *credit_report["tradelines"]["open"],
        *credit_report["tradelines"]["late_payments"],
        *credit_report["tradelines"]["default"],
    ]

    debt_amount = 0
    for tradeline in tradelines:
        if (
            tradeline["amount"]
            and utc_starttime < datetime.fromisoformat(tradeline["date"]) <= utc_endtime
        ):
            debt_amount += tradeline["amount"]

    if debt_amount == 0:
        return 0

    # Sum of credit transactions across all depository accounts in a given period as proxy as income
    institutions = api_client.institutions.get(user_uuid=user_uuid)
    credit_record_list = []
    for institution in institutions:
        transactions = api_client.transactions.get(
            user_uuid=user_uuid,
            institution_id=institution.institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_endtime,
        )
        credit_record_list.extend([dict(transaction) for transaction in transactions])

    # if no data available for the user, assume cash-in is zero
    if len(credit_record_list) == 0:
        return float("inf")

    credit_df = pd.DataFrame(credit_record_list)

    credit_amount = credit_df[
        (credit_df.impact == "CREDIT") & (credit_df.account_type == "depository")
    ].amount.sum()

    # Compute debt to income ratio
    ratio = debt_amount / credit_amount
    return ratio


if __name__ == "__main__":
    # Victor Lawal, vicotr@pngme.demo.com, 2437014328765
    user_uuid = "2ea8bbca-6e33-4d3f-9622-2e324045c272"

    token = os.environ["PNGME_TOKEN"]
    client = Client(token)

    now = datetime(2021, 9, 1)
    now_less_30 = now - timedelta(days=30)

    debt_to_income_ratio_0_30 = get_debt_to_income_ratio(
        api_client=client,
        user_uuid=user_uuid,
        utc_starttime=now_less_30,
        utc_endtime=now,
    )

    print(debt_to_income_ratio_0_30)
