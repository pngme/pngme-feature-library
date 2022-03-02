#!/usr/bin/env python3
"""
Net cash flow (inflow credit minus outflow debit) over a time period
for a given user over all their depository accounts.
Typical date ranges are last 30 days, 31-60 days and 61-90 days.
"""
from datetime import datetime
from datetime import timedelta

import os
from pngme.api import Client
import pandas as pd


def get_net_cash_flow(
    api_client: Client, user_uuid: str, utc_starttime: datetime, utc_endtime: datetime
) -> float:
    """Compute the net cash flow across all depository accounts for a given user in a given period.
    No currency conversions are performed.

    Args:
        api_client: Pngme API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the datetime for the left-hand-side of the time-window
        utc_endtime: the datetime for the right-hand-side of the time-window
    Returns:
        the sum total of all credit transaction amounts
    """
    account_details = api_client.accounts.get(user_uuid=user_uuid)

    # Constructs a dataframe that contains transactions from all accounts for the user
    record_list = []
    for individual_account in account_details:
        transactions = api_client.transactions.get(
            user_uuid=user_uuid,
            account_uuid=individual_account.acct_uuid,
            utc_starttime=utc_starttime,
            utc_endtime=utc_endtime,
        )
        record_list.extend([dict(transaction) for transaction in transactions])

    # if no data available for the user, assume cash-in is zero
    if len(record_list) == 0:
        return 0.0

    record_df = pd.DataFrame(record_list)

    # Get the total inbound credit over a period
    inflow_amount = record_df[
        (record_df.impact == "CREDIT")  # Subset by credit
        & (
            record_df.account_type.isin(["depository"])
        )  # Only consider depository account
    ].amount.sum()

    # Get the total outbound debit over a period
    outflow_amount = record_df[
        (record_df.impact == "DEBIT")  # Subset by debit
        & (record_df.account_type.isin(["depository"]))  # No loan related account
    ].amount.sum()

    total_net_cash_flow = inflow_amount - outflow_amount
    return total_net_cash_flow


if __name__ == "__main__":

    USER_UUID = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"  # Mercy Otieno, mercy@pngme.demo.com, 254123456789

    client = Client(access_token=os.environ["PNGME_TOKEN"])

    decision_time = datetime(2021, 10, 1)
    decision_time_less_30 = decision_time - timedelta(days=30)
    decision_time_less_60 = decision_time - timedelta(days=60)
    decision_time_less_90 = decision_time - timedelta(days=90)

    net_cash_flow_0_30 = get_net_cash_flow(
        client, USER_UUID, decision_time_less_30, decision_time
    )
    net_cash_flow_31_60 = get_net_cash_flow(
        client, USER_UUID, decision_time_less_60, decision_time_less_30
    )
    net_cash_flow_61_90 = get_net_cash_flow(
        client, USER_UUID, decision_time_less_90, decision_time_less_60
    )
