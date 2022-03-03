#!/usr/bin/env python3
"""
Sums minimum balances across all depository accounts for a given user over a time period.
"""

from datetime import datetime
from datetime import timedelta
import os
from typing import Optional
from pngme.api import Client


def sum_of_account_minimum_balances(
    api_client: Client, user_uuid: str, utc_starttime: datetime, utc_endtime: datetime
) -> Optional[float]:
    """Compute the sum of observed minimum balances for all depository accounts

    Args:
        api_client: Pngme API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the datetime for the left-hand-side of the time-window
        utc_endtime: the datetime for the right-hand-side of the time-window

    Returns:
        Sum of minimum balance observed for all depository accounts, in a given time window.
        If balance information was observed, it returns None.
    """

    sum_of_minimum_balances = 0
    observed_balance = False  # Whether observed any depository account balance

    institutions = api_client.institutions.get(user_uuid)
    for institution in institutions:
        institution_id = institution.institution_id
        balances = api_client.balances.get(
            user_uuid,
            institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_endtime,
        )
        account_numbers = {balance.account_number for balance in balances}
        for account_number in account_numbers:
            depository_balances = [
                balance.balance
                for balance in balances
                if balance.account_type == "depository"
                and balance.account_number == account_number
            ]

            balance_min = (
                min(depository_balances) if len(depository_balances) > 0 else None
            )

            if balance_min is not None:
                observed_balance = True
                sum_of_minimum_balances += balance_min

    if observed_balance is False:
        return None

    return sum_of_minimum_balances


if __name__ == "__main__":

    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    USER_UUID = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    client = Client(access_token=os.environ["PNGME_TOKEN"])

    decision_time = datetime(2021, 10, 1)
    decision_time_less_30 = decision_time - timedelta(days=30)
    decision_time_less_60 = decision_time - timedelta(days=60)
    decision_time_less_90 = decision_time - timedelta(days=90)

    sum_of_account_minimum_balances_0_30 = sum_of_account_minimum_balances(
        client, USER_UUID, decision_time_less_30, decision_time
    )
    sum_of_account_minimum_balances_31_60 = sum_of_account_minimum_balances(
        client, USER_UUID, decision_time_less_60, decision_time_less_30
    )
    sum_of_account_minimum_balances_61_90 = sum_of_account_minimum_balances(
        client, USER_UUID, decision_time_less_90, decision_time_less_60
    )
