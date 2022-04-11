#!/usr/bin/env python3

import asyncio
import os
from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd  # type: ignore
from pngme.api import AsyncClient, Institution, TransactionRecord


async def get_institutions(
    api_client: AsyncClient, user_uuid: str
) -> List[Institution]:
    institutions = await api_client.institutions.get(user_uuid=user_uuid)
    return institutions


async def get_depository_transactions(
    api_client: AsyncClient, user_uuid: str, institutions: List[Institution]
) -> List[TransactionRecord]:
    depository_institutions = [
        institution
        for institution in institutions
        if "depository" in institution.account_types
    ]
    transactions = []
    for institution in depository_institutions:
        institution_id = institution.institution_id
        transactions.extend(
            await api_client.transactions.get(
                user_uuid=user_uuid,
                institution_id=institution_id,
            )
        )
    return transactions


def get_net_cash_flow(
    depository_transactions: List[TransactionRecord],
    utc_starttime: datetime,
    utc_endtime: datetime,
) -> Optional[float]:
    """Compute the net cash flow for a user over a given period.

    No currency conversions are performed. Typical date ranges are last 30 days, 31-60
    days and 61-90 days. Net cash flow is calculated by differencing cash-in (credit)
    and cash-out (debit) transactions across all of a user's depository accounts during
    the given period.

    Args:
        transactions: list of TransactionRecords returned by client.transactions.get()
        utc_starttime: the datetime for the left-hand-side of the time-window
        utc_endtime: the datetime for the right-hand-side of the time-window
    Returns:
        the sum total of all credit transaction amounts
        None if there is no transaction data
    """
    if len(depository_transactions) == 0:
        return None

    transaction_df = pd.DataFrame([transaction.dict() for transaction in depository_transactions])

    # Filter transactions by time
    is_within_time_window = (transaction_df.ts >= utc_starttime.timestamp()) & (
        transaction_df.ts <= utc_endtime.timestamp()
    )
    transaction_df = transaction_df.loc[is_within_time_window]

    # Get the total cash-in (credit) amount over a period
    cash_in_amount = transaction_df[(transaction_df.impact == "CREDIT")].amount.sum()

    # Get the total cash-out (debit) amount over a period
    cash_out_amount = transaction_df[(transaction_df.impact == "DEBIT")].amount.sum()

    total_net_cash_flow = cash_in_amount - cash_out_amount
    return total_net_cash_flow


async def main(api_client: AsyncClient):
    institutions = await get_institutions(api_client=api_client, user_uuid=user_uuid)
    depository_transactions = await get_depository_transactions(
        api_client=api_client,
        user_uuid=user_uuid,
        institutions=institutions,
    )

    decision_time = datetime(2021, 10, 1)
    decision_time_less_30 = decision_time - timedelta(days=30)
    decision_time_less_60 = decision_time - timedelta(days=60)
    decision_time_less_90 = decision_time - timedelta(days=90)

    net_cash_flow_0_30 = get_net_cash_flow(
        depository_transactions=depository_transactions,
        utc_starttime=decision_time_less_30,
        utc_endtime=decision_time,
    )
    net_cash_flow_31_60 = get_net_cash_flow(
        depository_transactions=depository_transactions,
        utc_starttime=decision_time_less_60,
        utc_endtime=decision_time_less_30,
    )
    net_cash_flow_61_90 = get_net_cash_flow(
        depository_transactions=depository_transactions,
        utc_starttime=decision_time_less_90,
        utc_endtime=decision_time_less_60,
    )

    return {
        "net_cash_flow_0_30": net_cash_flow_0_30,
        "net_cash_flow_31_60": net_cash_flow_31_60,
        "net_cash_flow_61_90": net_cash_flow_61_90,
    }


if __name__ == "__main__":
    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    api_client = AsyncClient(access_token=token)

    result = asyncio.run(main(api_client))
    print(result)
    # {'net_cash_flow_0_30': 20240, 'net_cash_flow_31_60': 21737, 'net_cash_flow_61_90': -900}
