#!/usr/bin/env python3

import os
from typing import Optional

from pngme.api import Client


def get_sum_of_open_and_late_loan_balances(
    client: Client, user_uuid: str
) -> Optional[float]:
    """Return the sum of open and late-payment loan balances

    Uses the credit report resource to sum the value of open and late payment
    tradelines to estimate current loan balance.

    Args:
        client: Pngme API client
        user_uuid: Pngme mobile phone user_uuid

    Returns:
        Total open and late loan balances, or None if a credit report cannot be
        generated for the given user.
    """

    credit_report = client.credit_report.get(user_uuid)

    if not credit_report:
        return None

    # Sum the values of all open and late_payment tradelines.
    tradedlines = [
        *credit_report["tradelines"]["open"],
        *credit_report["tradelines"]["late_payments"],
    ]

    total_loan_balances = 0
    for tradeline in tradedlines:
        amount = tradeline["amount"]
        if amount:
            total_loan_balances += amount

    return total_loan_balances


if __name__ == "__main__":
    # Fehintolu Abebayo, fehintolu@pngme.demo.com, 2341234567890
    user_uuid = "1b333f2c-d700-4ac6-b9a7-7966526cd47c"

    token = os.environ["PNGME_TOKEN"]
    client = Client(token)

    sum_of_open_and_late_loan_balances = get_sum_of_open_and_late_loan_balances(
        client=client, user_uuid=user_uuid
    )

    print(sum_of_open_and_late_loan_balances)
