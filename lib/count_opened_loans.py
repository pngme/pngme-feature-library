#!/usr/bin/env python3
"""
Counts the number of unique institutions that approved and/or disbursed
one or more loans over a time period, for a given user.
Typical date range is last 30 days.
"""
from datetime import datetime
from datetime import timedelta
from pngme.api import Client

import pandas as pd


def count_opened_loans(
    api_client: Client, user_uuid: str, epoch_start: datetime, epoch_end: datetime
) -> int:
    """Count number of unique institutions in approved or disbursed loans in a period

    Args:
        api_client: Pngme API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        epoch_start: datetime for the left-hand-side of the time-window
        epoch_end: datetime for the right-hand-side of the time-window

    Returns: Int
    """

    # Get account information
    account_responses = api_client.accounts.get(user_uuid=user_uuid)

    # Loop through account for collecting loan approval and disbursement alert info
    alert_records = []
    for individual_account in account_responses:
        account_uuid = individual_account.acct_uuid
        alert_response = api_client.alerts.get(
            user_uuid=user_uuid,
            account_uuid=account_uuid,
            labels=["LoanApproved", "LoanDisbursed"],
            utc_starttime=epoch_start,
            utc_endtime=epoch_end,
        )
        for entry in alert_response:
            entry_dict = dict(entry)
            entry_dict["account_uuid"] = account_uuid
            alert_records.append(entry_dict)
    # Construct alert record dataframe
    alert_df = pd.DataFrame(alert_records)

    if alert_df.empty:
        return 0

    return alert_df["account_uuid"].nunique()


if __name__ == "__main__":

    API_TOKEN = "MY_API_TOKEN"  # paste token here to run script
    USER_UUID = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    client = Client(access_token=API_TOKEN)

    utc_endtime = datetime(2022, 3, 1)
    utc_starttime = utc_endtime - timedelta(days=30)
    # For a short period time, number of unique accounts in the record
    # is a good approximation for approved/disbursed loan count
    count_opened_loans_0_30 = count_opened_loans(
        api_client=client,
        user_uuid=USER_UUID,
        epoch_start=utc_starttime,
        epoch_end=utc_endtime,
    )
