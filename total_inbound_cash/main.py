import os
from datetime import datetime, timedelta

import pandas as pd
from pngme.api import Client

token = os.environ["PNGME_TOKEN"]
client = Client(token)


# Fetch a list of a user's financial accounts.
user_uuid = "33b6215d-3d75-4271-801c-6da27603a8be"
accounts = client.accounts.get(user_uuid)


# Fetch transactions across all accounts for the last 90 days.
utc_endtime = datetime(2021, 12, 31)
utc_starttime = utc_endtime - timedelta(days=90)

transactions = []
for account in accounts:
    account_uuid = account.acct_uuid
    transactions_in_account = client.transactions.get(
        user_uuid,
        account_uuid,
        utc_starttime=utc_starttime,
        utc_endtime=utc_endtime
    )
    transactions = [*transactions, *transactions_in_account]


# Load transactions into a pandas dataframe.
transactions_df = pd.DataFrame([transaction.dict() for transaction in transactions])


# Calculate total inbound cash.
total_inbound_cash_last_90_days = transactions_df[
    # Include only credit transactions.
    (transactions_df.impact == "CREDIT")
    # Exclude loan related transactions.
    & (~transactions_df.account_type.isin(["loan", "revolving_loan"]))
].amount.sum()
print(total_inbound_cash_last_90_days)
