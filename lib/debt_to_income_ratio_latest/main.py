#!/usr/bin/env python3

import asyncio
import os
from datetime import datetime, timedelta
from pngme.api import AsyncClient


async def get_debt_to_income_ratio_latest(
    api_client: AsyncClient,
    user_uuid: str,
    utc_starttime: datetime,
    utc_endtime: datetime,    
) -> float:
    """Compute the debt to income ratio over a given period

    Debt: Sum of loan balances across all loan accounts over a given period
    Income: Sum credit transactions across all depository accounts over a given period.

    Args:
        api_client: Pngme Async API client
        user_uuid: Pngme mobile phone user_uuid
        utc_endtime: the time at which the latest debt_to_income ratio is computed
        utc_starttime: if balance hasn't been updated since this day, then balance record is stale, and method returns None

    Returns:
        Ratio of debt / income for the given user. Returning 0 would correspond to a 0 debt amount,
        and 'inf' would correspond to a 0 income amount.

        None means that there are no loan balances nor credit transactions for the given time period.
    """
    
    # STEP 1: fetch list of institutions belonging to the user
    institutions = await api_client.institutions.get(user_uuid=user_uuid)

    # subset to only fetch data for institutions known to contain loan-type accounts for the user
    institutions_w_loan = []
    for inst in institutions:
        if "loan" in inst["account_types"]:
            institutions_w_loan.append(inst)
    
    # STEP 2: Prepare a list of tasks to fetch balances for each institution with loan accounts
    loan_inst_coroutines = []
    for institution in institutions_w_loan:
        loan_inst_coroutines.append(
            await api_client.balances.get(
                user_uuid=user_uuid,
                institution_id=institution["institution_id"],
                utc_starttime=utc_starttime,
                utc_endtime=utc_endtime,
                account_types=["loan"],
            )
        )

    # subset to only fetch data for institutions known to contain depository-type accounts for the user
    institutions_w_depository = []
    for inst in institutions:
        if "depository" in inst["account_types"]:
            institutions_w_depository.append(inst)

    # STEP 3: Prepare a list of tasks to fetch balances for each institution with depository accounts
    depository_inst_coroutines = []
    for institution in institutions_w_depository:
        depository_inst_coroutines.append(
            await api_client.transactions.get(
                user_uuid=user_uuid,
                institution_id=institution["institution_id"],
                utc_starttime=utc_starttime,
                utc_endtime=utc_endtime,
                account_types=["depository"],
            )
        )

    # STEP 4: Execute loan requests in parallel
    loan_balances_by_institution = await asyncio.gather(*loan_inst_coroutines)
    
    # STEP 4.1: Include institution id to each loan balance record so we can sum the balance for each one
    loan_records_list = []
    for ix, balance_records in enumerate(loan_balances_by_institution):
        institution_id = institutions[ix]["institution_id"]
        for balance in balance_records:
            balance_dict = dict(balance)
            balance_dict["institution_id"] = institution_id

            loan_records_list.append(balance_dict)

    # STEP 5: Execute depository requests in parallel
    depository_transactions_by_institution = await asyncio.gather(*depository_inst_coroutines)

    # STEP 5.1: Filter out transactions that are not credit transactions
    depository_credit_records_list = []
    for inst_lst in depository_transactions_by_institution:
        for transaction_record in inst_lst:
            if transaction_record["impact"] == "CREDIT":
                # As we needed to cast the balance records as dict, we do the same
                # here so calculations do not mix dictionaries and objects for easier reading
                depository_credit_records_list.append(dict(transaction_record))

    # STEP 5.2: Early exit for edge cases
    
    ## if there is no updated balance since the start time, then return None
    if not loan_records_list and not depository_credit_records_list:
        return None

    ## if no data available for the user, assume sum of loan balances to be zero
    if len(loan_records_list) == 0:
        return 0.0

    ## if no data available for the user, assume cash-in is zero
    if len(depository_credit_records_list) == 0:
        return float("inf")

    # STEP 6: Here we sort by timestamp so latest balances are on top
    loan_records_list = sorted(loan_records_list, key=lambda x: x["ts"], reverse=True)

    # STEP 7: Then we loop through all balances per institution and account and store the latest balance
    latest_balances = {}
    for loan_record in loan_records_list:
        key = (loan_record["institution_id"], loan_record["account_id"])
        if key not in latest_balances:
            latest_balances[key] = loan_record["balance"]
    
    # STEP 8: Sum all the balances
    sum_of_loan_balances_latest = sum(latest_balances.values())

    # STEP 9: Sum of credit transactions across all depository accounts
    sum_of_depository_credit_transactions = 0
    for credit in depository_credit_records_list:
        sum_of_depository_credit_transactions += credit["amount"]

    ## Early return if sum of transactions is zero
    ### (This is an extremely rare case where all credit transactions add up to zero,
    ###  which could happen if parsed amounts are zero or if the institution has
    ###  negative credit transactions)
    if sum_of_depository_credit_transactions == 0:
        return float('inf')
    # STEP 10: Compute debt to income ratio
    ratio = sum_of_loan_balances_latest / sum_of_depository_credit_transactions
    return ratio


if __name__ == "__main__":
    # Victor Lawal, vicotr@pngme.demo.com, 2437014328765
    user_uuid = "2ea8bbca-6e33-4d3f-9622-2e324045c272"

    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    now = datetime(2021, 9, 1)
    cutoff_interval = timedelta(days=30)
    utc_starttime = now - cutoff_interval

    async def main():
        debt_to_income_ratio_latest = await get_debt_to_income_ratio_latest(
            api_client=client,
            user_uuid=user_uuid,
            utc_starttime=utc_starttime,
            utc_endtime=now,
        )

        print(debt_to_income_ratio_latest)

    asyncio.run(main())
