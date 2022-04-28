#!/usr/bin/env python3

import asyncio
import os
from datetime import datetime, timedelta
from pngme.api import AsyncClient


async def get_debt_to_income_ratio_latest(
    api_client: AsyncClient,
    user_uuid: str,
    utc_time: datetime,
    cutoff_interval: timedelta = timedelta(days=30),
) -> float:
    """Compute the debt to income ratio over a given period

    Debt: Sum of loan balances across all loan accounts over a given period
    Income: Sum credit transactions across all depository accounts over a given period.

    Args:
        api_client: Pngme Async API client
        user_uuid: Pngme mobile phone user_uuid
        utc_time: the time at which the latest debt_to_income ratio is computed
        cutoff_interval: if balance hasn't been updated within this interval, then balance record is stale, and method returns 0

    Returns:
        Ratio of debt / income for the given user. Returning 0 would correspond to a 0 debt amount,
        and Inf would correspond to a 0 income amount.
    """

    institutions = await api_client.institutions.get(user_uuid=user_uuid)
    utc_starttime = utc_time - cutoff_interval

    # subset to only fetch data for institutions known to contain loan-type accounts for the user
    institutions_w_loan = [
        inst for inst in institutions if "loan" in inst.account_types
    ]

    loan_inst_coroutines = [
        api_client.balances.get(
            user_uuid=user_uuid,
            institution_id=institution.institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_time,
            account_types=["loan"],
        )
        for institution in institutions_w_loan
    ]

    # subset to only fetch data for institutions known to contain depository-type accounts for the user
    institutions_w_depository = [
        inst for inst in institutions if "depository" in inst.account_types
    ]

    depository_inst_coroutines = [
        api_client.transactions.get(
            user_uuid=user_uuid,
            institution_id=institution.institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_time,
            account_types=["depository"],
        )
        for institution in institutions_w_depository
    ]

    response = await asyncio.gather(*loan_inst_coroutines, *depository_inst_coroutines)
    loan_balances = response[: len(loan_inst_coroutines)]
    depository_transactions = response[len(loan_inst_coroutines) :]

    loan_records_list = []
    depository_credit_records_list = []

    for ix, inst_list in enumerate(loan_balances):
        institution_id = institutions[ix].institution_id
        loan_records_list.extend(
            [
                dict(transaction, institution_id=institution_id)
                for transaction in inst_list
            ]
        )

    for inst_lst in depository_transactions:
        depository_credit_records_list.extend(
            [
                dict(transaction)
                for transaction in inst_lst
                if transaction.impact == "CREDIT"
            ]
        )

    # if no data available for the user, assume sum of loan balances to be zero
    if len(loan_records_list) == 0:
        return 0.0

    # if no data available for the user, assume cash-in is zero
    if len(depository_credit_records_list) == 0:
        return float("inf")

    # Here we sort by timestamp so latest balances are on top
    loan_records_list = sorted(loan_records_list, key=lambda x: x["ts"], reverse=True)

    # Then we loop through all balances per institution and account and store the latest balance
    latest_balances = {}
    for loan_record in loan_records_list:
        key = (loan_record["institution_id"], loan_record["account_id"])
        if key not in latest_balances:
            latest_balances[key] = loan_record["balance"]
    
    # Finally, we can sum all the balances
    sum_of_loan_balances_latest = sum(latest_balances.values())

    # Sum of credit transactions across all depository accounts
    sum_of_depository_credit_transactions = 0
    for credit in depository_credit_records_list:
        sum_of_depository_credit_transactions += credit["amount"]

    # Compute debt to income ratio
    ratio = sum_of_loan_balances_latest / sum_of_depository_credit_transactions
    return ratio


if __name__ == "__main__":
    # Victor Lawal, vicotr@pngme.demo.com, 2437014328765
    user_uuid = "2ea8bbca-6e33-4d3f-9622-2e324045c272"

    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    now = datetime(2021, 9, 1)

    async def main():
        debt_to_income_ratio_latest = await get_debt_to_income_ratio_latest(
            api_client=client,
            user_uuid=user_uuid,
            utc_time=now,
        )

        print(debt_to_income_ratio_latest)

    asyncio.run(main())
