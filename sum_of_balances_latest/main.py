#!/usr/bin/env python3

import os
from datetime import datetime, timedelta

from pngme.api import Client


def get_sum_of_balances_latest(
    client: Client, user_uuid: str, utc_starttime: datetime, utc_endtime: datetime
) -> float:
    """Return the latest balance within the time range summed across all accounts.

    If an account does not contain any balance notifications within the time window,
    it is considered stale and not included in the total balance.

    Args:
        client: Pngme API client
        user_uuid: Pngme mobile phone user_uuid
        utc_starttime: start of the time window
        utc_endtime: end of the time window
    """
    institutions = client.institutions.get(user_uuid)

    # Sum the most recent balances from each account within our time window.
    sum_of_balances_latest = 0
    for institution in institutions:
        institution_id = institution.institution_id
        balances = client.balances.get(
            user_uuid,
            institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_endtime,
        )

        # Add latest account balance for each account within the institution.
        account_numbers = {balance.account_number for balance in balances}
        for account_number in account_numbers:
            depository_balances = [
                balance
                for balance in balances
                if balance.account_type == "depository"
                and balance.account_number == account_number
            ]
            if depository_balances:
                # Find latest balance record by sorting in time
                latest_balance = sorted(
                    depository_balances,
                    key=lambda balance: balance.ts,
                    reverse=True,
                )[0]
                sum_of_balances_latest += latest_balance.balance

    return sum_of_balances_latest


if __name__ == "__main__":
    token = os.environ["PNGME_TOKEN"]
    client = Client(token)

    # Mercy Otingo, mercy@pngme.demo, 234112312
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"
    utc_endtime = datetime(2021, 10, 31)
    utc_starttime = utc_endtime - timedelta(days=30)

    sum_of_balances_latest = get_sum_of_balances_latest(
        client=client,
        user_uuid=user_uuid,
        utc_starttime=utc_starttime,
        utc_endtime=utc_endtime,
    )
    print(sum_of_balances_latest)
