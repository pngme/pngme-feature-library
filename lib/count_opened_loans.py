#!/usr/bin/env python3
"""
Counts the total number of loans approved and disbursed over a time period, for a given user.
Typical date ranges are last 30 days,
"""
from pngme.api import Client

import pandas as pd
import pendulum


API_TOKEN = "MY_API_TOKEN"  # paste token here to run script


def _construct_alert_df(api_client: Client, user_uuid: str) -> pd.DataFrame:
    """Construct user approved or disbursed loan dataframes
    Args:
        api_client: Pngme API client
        user_uuid: the Pngme user_uuid for the mobile phone user
    Returns:
        DataFrame contains alert information
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
        )
        for entry in alert_response:
            entry_dict = dict(entry)
            entry_dict["account_uuid"] = account_uuid
            alert_records.append(entry_dict)
    return pd.DataFrame(alert_records)


def count_opened_loans(
    api_client: Client, user_uuid: str, epoch_start: int, epoch_end: int
) -> int:
    """Count number of unique institutions in approved or disbursed loans in a period

    Args:
        api_client: Pngme API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        epoch_start: the UTC epoch timestamp for the left-hand-side of the time-window
        epoch_end: the UTC epoch timestamp for the left-hand-side of the time-window

    Returns: Int
    """
    alert_df = _construct_alert_df(api_client=api_client, user_uuid=user_uuid)
    if alert_df.empty:
        return 0

    return alert_df[alert_df["ts"].between(epoch_start, epoch_end, inclusive="right")][
        "account_uuid"
    ].nunique()


if __name__ == "__main__":

    USER_UUID = "c9f0624d-4e7a-41cc-964d-9ea3b268427f"
    client = Client(access_token=API_TOKEN)

    now = pendulum.now()
    now_less_30 = now.subtract(days=30)
    # For a short period time, number of unique accounts in the record
    # is a good approximation for approved/disbursed loan count
    num_opened_loan_0_30_days = count_opened_loans(
        api_client=client,
        user_uuid=USER_UUID,
        epoch_start=now_less_30.int_timestamp,
        epoch_end=now.int_timestamp,
    )

    df = pd.DataFrame(
        {
            "user_uuid": USER_UUID,
            "number_of_opened_loans_0_30": num_opened_loan_0_30_days,
        }
    )

    print(df.to_markdown())
