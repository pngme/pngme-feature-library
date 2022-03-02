#!/usr/bin/env python3

import os
from datetime import datetime, timedelta

from pngme.api import Client


def get_sum_of_balances_latest(
    client: Client, user_uuid: str, utc_starttime: datetime, utc_endtime: datetime
) -> float:
    """Return the latest balance summed across all accounts."""
    accounts = client.accounts.get(user_uuid)

    # Sum the most recent (within 30 day window) balance in each account.
    sum_of_balances_latest = 0
    for account in accounts:
        account_uuid = account.acct_uuid
        balances = client.balances.get(
            user_uuid,
            account_uuid,
            utc_starttime=utc_starttime,
            utc_endtime=utc_endtime,
        )

        # Include only depository accounts
        depository_balances = [
            balance for balance in balances if balance.account_type == "depository"
        ]

        if depository_balances:
            # Sort balances descending in time, so the first is the most recent.
            depository_balances_sorted = sorted(
                depository_balances,
                key=lambda balance: balance.ts,
                reverse=True,
            )
            sum_of_balances_latest += depository_balances_sorted[0].balance

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
