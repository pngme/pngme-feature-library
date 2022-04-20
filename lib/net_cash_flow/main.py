#!/usr/bin/env python3

import os
from datetime import datetime, timedelta
from typing import Optional

from pngme.api import Client


def get_net_cash_flow(
    api_client: Client, user_uuid: str, utc_starttime: datetime, utc_endtime: datetime
) -> Optional[float]:
    """Compute the net cash flow for a user over a given period.

    No currency conversions are performed. Typical date ranges are last 30 days, 31-60
    days and 61-90 days. Net cash flow is calculated by differencing cash-in (credit)
    and cash-out (debit) transactions across all of a user's depository accounts during
    the given period.

    Args:
        api_client: Pngme API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the datetime for the left-hand-side of the time-window
        utc_endtime: the datetime for the right-hand-side of the time-window
    Returns:
        the sum total of all credit transaction amounts
        None if there is no transaction data
    """
    institutions = api_client.institutions.get(user_uuid=user_uuid)

    # subset to only fetch data for institutions known to contain depository-type accounts for the user
    institutions_w_depository = [
        inst for inst in institutions if "depository" in inst.account_types
    ]

    # Constructs a dataframe that contains transactions from all accounts for the user
    record_list = []
    for institution in institutions_w_depository:
        institution_id = institution.institution_id
        transactions = api_client.transactions.get(
            user_uuid=user_uuid,
            institution_id=institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_endtime,
            account_types=["depository"],
        )
        record_list.extend([dict(transaction) for transaction in transactions])

    # if no data available for the user, return None
    if len(record_list) == 0:
        return None

    # Get the total cash-in (credit) amount over a period
    cash_in_amount = sum([r["amount"] for r in record_list if r["impact"] == "CREDIT"])

    # Get the total cash-out (debit) amount over a period
    cash_out_amount = sum([r["amount"] for r in record_list if r["impact"] == "DEBIT"])

    total_net_cash_flow = cash_in_amount - cash_out_amount

    return total_net_cash_flow


if __name__ == "__main__":
    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = Client(access_token=token)

    decision_time = datetime(2021, 10, 1)
    decision_time_less_30 = decision_time - timedelta(days=30)
    decision_time_less_60 = decision_time - timedelta(days=60)
    decision_time_less_90 = decision_time - timedelta(days=90)

    net_cash_flow_0_30 = get_net_cash_flow(
        api_client=client,
        user_uuid=user_uuid,
        utc_starttime=decision_time_less_30,
        utc_endtime=decision_time,
    )
    net_cash_flow_31_60 = get_net_cash_flow(
        api_client=client,
        user_uuid=user_uuid,
        utc_starttime=decision_time_less_60,
        utc_endtime=decision_time_less_30,
    )
    net_cash_flow_61_90 = get_net_cash_flow(
        api_client=client,
        user_uuid=user_uuid,
        utc_starttime=decision_time_less_90,
        utc_endtime=decision_time_less_60,
    )

    print(net_cash_flow_0_30)
    print(net_cash_flow_31_60)
    print(net_cash_flow_61_90)
