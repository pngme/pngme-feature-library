#!/usr/bin/env python3

import os
from datetime import datetime, timedelta
from typing import Optional

from pngme.api import Client


def get_net_cash_flow(
    api_client: Client, user_uuid: str, utc_starttime: datetime, utc_endtime: datetime
) -> Optional[float]:
    """Compute the net cash flow for a user over a given period.

    No currency conversions are performed. Net cash flow is calculated by
    differencing cash-in (credit) and cash-out (debit) transactions across
    all of a user's depository accounts during the given period.

    Args:
        api_client: Pngme API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the datetime for the left-hand-side of the time-window
        utc_endtime: the datetime for the right-hand-side of the time-window
    Returns:
        the sum total of all credit transaction amounts
        None if there is no transaction data
    """
    # STEP 1: fetch list of institutions belonging to the user
    institutions = api_client.institutions.get(user_uuid=user_uuid)

    # subset to only fetch data for institutions known to contain depository-type accounts for the user
    institutions_w_depository = []
    for inst in institutions:
        if "depository" in inst.account_types:
            institutions_w_depository.append(inst)

    # STEP 2: Loop through all transactions adding the transactions
    cash_in_amount = 0
    cash_out_amount = 0
    for institution in institutions_w_depository:
        institution_id = institution.institution_id
        transactions = api_client.transactions.get(
            user_uuid=user_uuid,
            institution_id=institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_endtime,
            account_types=["depository"],
        )
        for transaction in transactions:
            if transaction.impact == "CREDIT":
                cash_in_amount += transaction.amount
            elif transaction.impact == "DEBIT":
                cash_out_amount += transaction.amount

    # If there are no transactions, return None
    if cash_in_amount == 0 and cash_out_amount == 0:
        return None

    # STEP 3: Compute the net cash flow as the difference between cash-in and cash-out
    total_net_cash_flow = cash_in_amount - cash_out_amount

    return total_net_cash_flow


if __name__ == "__main__":
    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = Client(access_token=token)

    utc_endtime = datetime(2021, 10, 1)
    utc_starttime = utc_endtime - timedelta(days=30)

    net_cash_flow = get_net_cash_flow(
        api_client=client,
        user_uuid=user_uuid,
        utc_starttime=utc_starttime,
        utc_endtime=utc_endtime,
    )

    print(net_cash_flow)
