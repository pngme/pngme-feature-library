#!/usr/bin/env python3

import os

from pngme.api import Client


def get_nondefault_loan_balance(client: Client, user_uuid: str) -> float:
    """Return the sum of nondefault tradelines.

    Uses the credit report resource to sum the value of open and late payment
    tradelines to estimate current loan balance.

    Args:
        client: Pngme API client
        user_uuid: Pngme mobile phone user_uuid
    """

    credit_report = client.credit_report.get(user_uuid)

    # Sum the values of all open and late_payment tradelines.
    nondefault_tradelines = [
        *credit_report["tradelines"]["open"],
        *credit_report["tradelines"]["late_payments"],
    ]

    nondefault_loan_balance = 0
    for tradeline in nondefault_tradelines:
        amount = tradeline["amount"]
        if amount:
            nondefault_loan_balance += amount

    return nondefault_loan_balance


if __name__ == "__main__":
    token = os.environ["PNGME_TOKEN"]
    client = Client(token)

    # Fehintolu Abebayo, fehintolu@pngme.demo.com, 2341234567890
    user_uuid = "1b333f2c-d700-4ac6-b9a7-7966526cd47c"

    nondefault_loan_balance = get_nondefault_loan_balance(
        client=client, user_uuid=user_uuid
    )
    print(nondefault_loan_balance)
