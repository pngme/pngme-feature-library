#!/usr/bin/env python3
"""
Counts the number of unique institutions with which a user has opened one or more loans
for a given user over a period.
Typical date range is last 30 days.
"""
from datetime import datetime
from datetime import timedelta
import os
from pngme.api import Client


def count_opened_loans(
    api_client: Client, user_uuid: str, utc_starttime: datetime, utc_endtime: datetime
) -> int:
    """Count number of unique institutions in approved or disbursed loans in a perio

    For a short period time, number of unique accounts in the record is a
    good approximation for approved/disbursed loan count

    Args:
        api_client: Pngme API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: datetime for the left-hand-side of the time-window
        utc_endtime: datetime for the right-hand-side of the time-window

    Returns:
        Number of unique institutions
    """

    # Get account information
    accounts = api_client.accounts.get(user_uuid=user_uuid)

    # Loop through account for collecting loan approval and disbursement alert info
    num_institutions = 0
    for account in accounts:
        account_uuid = account.acct_uuid
        alerts = api_client.alerts.get(
            user_uuid=user_uuid,
            account_uuid=account_uuid,
            labels=["LoanApproved", "LoanDisbursed"],
            utc_starttime=utc_starttime,
            utc_endtime=utc_endtime,
        )
        if len(alerts) > 0:
            num_institutions += 1

    return num_institutions


if __name__ == "__main__":

    USER_UUID = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    client = Client(access_token=os.environ["PNGME_TOKEN"])

    now = datetime(2022, 3, 1)
    now_less_30 = now - timedelta(days=30)

    count_opened_loans_0_30 = count_opened_loans(
        api_client=client,
        user_uuid=USER_UUID,
        utc_starttime=now_less_30,
        utc_endtime=now,
    )
