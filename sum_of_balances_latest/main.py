import os
from datetime import datetime, timedelta

from pngme.api import Client

token = os.environ["PNGME_TOKEN"]
client = Client(token)


# Fetch a list of a user's financial accounts.
user_uuid = "33b6215d-3d75-4271-801c-6da27603a8be"
accounts = client.accounts.get(user_uuid)


# Fetch balances for each account within the last 30 days.
utc_endtime = datetime(2021, 12, 31)
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
        balances_sorted_by_time_descending = sorted(
            balances,
            key=lambda balance: balance.ts,
            reverse=True,
        )
        sum_of_balances_latest += balances_sorted_by_time_descending[0].balance


print(sum_of_balances_latest)
