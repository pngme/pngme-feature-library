#!/usr/bin/env python3
"""
Sum of default loan balances
"""

import os

from pngme.api import Client


def get_sum_of_default_loan_balances(api_client: Client, user_uuid: str) -> float:
    """Return the sum of default tradelines.

    Uses the credit report resource to sum the value of default tradelines
    to estimate current loan balance.

    Args:
        api_client: Pngme API client
        user_uuid: Pngme mobile phone user_uuid
    """
    credit_report = api_client.credit_report.get(user_uuid)
    default_tradelines = credit_report["tradelines"]["default"]
    default_loan_balance = 0
    for tradeline in default_tradelines:
        if tradeline["amount"]:
            default_loan_balance += tradeline["amount"]

    return default_loan_balance


if __name__ == "__main__":
    # Fehintolu Abebayo, fehintolu@pngme.demo.com, 2341234567890
    USER_UUID = "1b333f2c-d700-4ac6-b9a7-7966526cd47c"

    token = os.environ["PNGME_TOKEN"]
    client = Client(token)

    sum_of_default_loan_balances = get_sum_of_default_loan_balances(
        api_client=client, user_uuid=USER_UUID
    )
    print(sum_of_default_loan_balance)
