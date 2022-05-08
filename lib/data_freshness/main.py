#!/usr/bin/env python3

import asyncio
import os
from datetime import datetime, timedelta
from pngme.api import AsyncClient

# add a new feature to the feature library named freshness.
# freshness is calculated as the difference between the utc_time provided and the largest toc observed in the financial event records or alert records.
# implementation should be something like:
# asynchronously query /balances, /transactions and /alerts over all time. only take the first page (should be sorted by toc desc).
# take the largest toc among the first pages.
# subtract utc_time from that toc, convert to minutes (integer) and return.
# --
# also, very importantly, confirm that the results are sorted as toc descending, so we know authoritatively that the first page contains the largest toc.



async def get_data_freshness(
        api_client: AsyncClient,
        user_uuid: str,
        utc_starttime: datetime,
        utc_endtime: datetime,
) -> float:
    """Return the time in days between utc_endtime and the most recent financial event or alert,
    as an indicator of data freshness.
    Args:
        api_client: Pngme Async API client
        user_uuid: Pngme mobile phone user_uuid
        utc_starttime: the time from which balances are considered
        utc_endtime: the time until which balances are considered
    Returns:
        Return the time in days between utc_endtime and the most recent financial event or alert
    """

    # STEP 1: get a list of all institutions for the user
    institutions = await api_client.institutions.get(user_uuid=user_uuid)
    # subset to only fetch data for institutions known to contain loan-type accounts for the user
    institutions_w_loan = []
    for inst in institutions:
        if "loan" in inst.account_types:
            institutions_w_loan.append(inst)

    # STEP 2: get lists of transactions, alerts, and balances for each institution

    # STEP 2a: get a list of all transactions for each institution

    ## Don't need to do this by institution I believe!
    ## How to get all records, from paginated data...
    trans_inst_coroutines = []
    for inst in institutions_w_loan:
        trans_inst_coroutines.append(
            api_client.transactions.get(
                user_uuid=user_uuid,
                # institution_id=inst.institution_id, #Default to all.
                utc_starttime=utc_starttime,
                utc_endtime=utc_endtime,
                # account_types=["loan"], #Default to all.
            )
        )
    transactions_by_institution = await asyncio.gather(*trans_inst_coroutines)

    # STEP 3: We flatten the lists of transactions into a single list of transactions
    transactions_flattened = []
    for ix, transactions in enumerate(transactions_by_institution):
        institution_id = institutions[ix].institution_id
        for transaction in transactions:
            transaction_dict = dict(transaction)
            transaction_dict["institution_id"] = institution_id

            transactions_flattened.append(transaction_dict)

    # STEP 5: Here we sort by timestamp so latest transactions are on top
    transactions_max_ts = max(transactions_flattened, key=lambda x: x["ts"], reverse=True)

    # transactions_flattened = sorted(transactions_flattened, key=lambda x: x["ts"], reverse=True)
    #
    # # STEP 6: Then we loop through all transactions per institution and account and store the latest transaction
    # latest_transactions = {}
    # for transaction_record in transactions_flattened:
    #     key = (transaction_record["institution_id"], transaction_record["account_id"])
    #     if key not in latest_transactions:
    #         # As we go top-down, we only need to store the first balance we found for each institution+account
    #         latest_transactions[key] = transaction_record["balance"]

    # STEP 2: get a list of all balances for each institution
    inst_coroutines = []
    for inst in institutions_w_loan:
        inst_coroutines.append(
            api_client.balances.get(
                user_uuid=user_uuid,
                institution_id=inst.institution_id,
                utc_starttime=utc_starttime,
                utc_endtime=utc_endtime,
                account_types=["loan"],
            )
        )

    balances_per_institution = await asyncio.gather(*inst_coroutines)

    # STEP 3: We flatten the lists of balances into a single list of balances
    balances_flattened = []
    for ix, balances in enumerate(balances_per_institution):
        institution_id = institutions[ix].institution_id
        for balance in balances:
            balances_flattened.append(dict(balance, institution_id=institution_id))

    # STEP 5: Here we sort by timestamp so latest balances are on top
    balances_flattened = sorted(balances_flattened, key=lambda x: x["ts"], reverse=True)

    # STEP 6: Then we loop through all balances per institution and account and store the latest balance
    latest_balances = {}
    for loan_record in balances_flattened:
        key = (loan_record["institution_id"], loan_record["account_id"])
        if key not in latest_balances:
            # As we go top-down, we only need to store the first balance we found for each institution+account
            latest_balances[key] = loan_record["balance"]

    # STEP 7: Finally, we can sum all the balances
    balances_latest = sum(latest_balances.values())



    # STEP 7: Finally, we can get the minimum of the data freshness (in days)
    data_freshness = min(data_freshness.values())
    return data_freshness

if __name__ == "__main__":
    # Mercy Otingo, mercy@pngme.demo, 234112312
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"
    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)
    now = datetime(2021, 10, 31)
    cutoff_interval = timedelta(days=30)
    utc_starttime = now - cutoff_interval


    async def main():
        sum_of_balances_latest = await get_sum_of_loan_balances_latest(
            api_client=client,
            user_uuid=user_uuid,
            utc_starttime=utc_starttime,
            utc_endtime=now,
        )
        print(sum_of_balances_latest)


    asyncio.run(main())