import os
from datetime import datetime, timedelta

from pngme.api import Client

token = os.environ["PNGME_TOKEN"]
client = Client(token)


# Fetch a list of a user's financial accounts.
user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"
accounts = client.accounts.get(user_uuid)


# Sum the most recent (within 30 day window) balance in each account.
utc_endtime = datetime(2021, 10, 31)
utc_starttime = utc_endtime - timedelta(days=30)

sum_of_balances_latest = 0
for account in accounts:
    account_uuid = account.acct_uuid
    balances = client.balances.get(
        user_uuid,
        account_uuid,
        utc_starttime=utc_starttime,
        utc_endtime=utc_endtime,
    )

    if balances:
        # Sort balances descending in time, so the first is the most recent.
        balances_sorted_by_time_descending = sorted(
            balances,
            key=lambda balance: balance.ts,
            reverse=True,
        )
        sum_of_balances_latest += balances_sorted_by_time_descending[0].balance


print(sum_of_balances_latest)
