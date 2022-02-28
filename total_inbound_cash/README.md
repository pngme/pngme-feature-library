# Total inbound cash

A user's total inbound cash can be a useful proxy for a user's income. We can calculate total inbound cash using Pngme's [`/accounts`](https://developers.api.pngme.com/reference/get_users-user-uuid-accounts-1) and [`/transactions`](https://developers.api.pngme.com/reference/get_users-user-uuid-accounts-acct-uuid-transactions-1) resources.

## Getting `/accounts` and `/transactions`

For a user of interest:

```python
user_uuid = "33b6215d-3d75-4271-801c-6da27603a8be"
```

We'll use Pngme's [python client](https://github.com/pngme/pngme-api) to get a list of the user's [`/accounts`](https://developers.api.pngme.com/reference/get_users-user-uuid-accounts-1) using your API `token` found in the [Pngme Dashboard](https://admin.pngme.com):

```python
from pngme.api import Client

token = "" # your API token
client = Client(token)

accounts = client.accounts.get(user_uuid)
```

## Calculating total inbound cash

Let's calculate the total inbound cash by summing credit [`/transactions`](https://developers.api.pngme.com/reference/get_users-user-uuid-accounts-acct-uuid-transactions-1) over all of the user's [`/accounts`](https://developers.api.pngme.com/reference/get_users-user-uuid-accounts-1).

We'll start by getting a `pd.DataFrame` containing the transactions across all of the user's [`/accounts`](https://developers.api.pngme.com/reference/get_users-user-uuid-accounts-1) for the period we're interested in (30 days):

```python
utc_starttime = datetime(2021, 12, 1)
utc_endtime = datetime(2021, 12, 31)

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

transactions_df = pd.DataFrame([transaction.dict() for transaction in transactions])
```

Then calculate total inbound cash by summing credit transactions while excluding loan related transactions:

```python
total_inbound_cash_last_30_days = transactions_df[
    # Filter for only credit transactions
    (transactions_df.impact == "CREDIT")
    # Exclude loan related transactions
    & (~transactions_df.account_type.isin(["loan", "revolving_loan"]))
].amount.sum() # TODO: specify currency
```

## Next steps

Try calculating the user's total inbound cash over the last 90 days. See [main.py](main.py) for the full code.

1. Create and activate a virtual environment:

```bash
python3 -m venv .venv
.venv/bin/activate
```

2. Install the dependencies:

```bash
pip3 install -r requirements.txt
```

3. Set the `PNGME_TOKEN` environment variable using your API token from [admin.pngme.com](https://admin.pngme.com):

```bash
export PNGME_TOKEN="eyJraWQiOiJcL3d..."
```

4. Run the example:

```bash
python3 main.py

# 93529
```
