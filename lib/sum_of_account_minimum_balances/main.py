#!/usr/bin/env python3

import os
from datetime import datetime, timedelta
from typing import Optional

from pngme.api import Client


def get_sum_of_account_minimum_balances(
    api_client: Client, user_uuid: str, utc_starttime: datetime, utc_endtime: datetime
) -> Optional[float]:
    """Sum observed minimum balances across all depository accounts.

    Args:
        api_client: Pngme API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the datetime for the left-hand-side of the time-window
        utc_endtime: the datetime for the right-hand-side of the time-window

    Returns:
        Sum of minimum balance observed for all depository accounts, in a given time window.
        If balance information was observed, it returns None.
    """

    sum_of_account_minimum_balances = 0
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
            if len(depository_balances) > 0:
                balance_min = min(depository_balances)
                observed_balance = True
                sum_of_account_minimum_balances += balance_min

    if observed_balance is False:
        return None

    return sum_of_account_minimum_balances


if __name__ == "__main__":
    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = Client(token)

    decision_time = datetime(2021, 10, 1)
    decision_time_less_30 = decision_time - timedelta(days=30)
    decision_time_less_60 = decision_time - timedelta(days=60)
    decision_time_less_90 = decision_time - timedelta(days=90)

    sum_of_account_minimum_balances_0_30 = get_sum_of_account_minimum_balances(
        api_client=client,
        user_uuid=user_uuid,
        utc_starttime=decision_time_less_30,
        utc_endtime=decision_time,
    )
    sum_of_account_minimum_balances_31_60 = get_sum_of_account_minimum_balances(
        api_client=client,
        user_uuid=user_uuid,
        utc_starttime=decision_time_less_60,
        utc_endtime=decision_time_less_30,
    )
    sum_of_account_minimum_balances_61_90 = get_sum_of_account_minimum_balances(
        api_client=client,
        user_uuid=user_uuid,
        utc_starttime=decision_time_less_90,
        utc_endtime=decision_time_less_60,
    )

    print(sum_of_account_minimum_balances_0_30)
    print(sum_of_account_minimum_balances_31_60)
    print(sum_of_account_minimum_balances_61_90)
