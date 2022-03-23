#!/usr/bin/env python3

import os
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd  # type: ignore
from pngme.api import Client


def get_standard_deviation_of_week_to_week_sum_of_credits(
    client: Client, user_uuid: str, utc_starttime: datetime, utc_endtime: datetime
) -> Optional[float]:
    """Compute the standard deviations of week-to-week sum of credits

    Args:
        client: Pngme API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the datetime for the left-hand-side of the time-window
        utc_endtime: the datetime for the right-hand-side of the time-window

    Returns:
        Standard deviation of week-to-week sum of credits
    """
    institutions = client.institutions.get(user_uuid=user_uuid)

    # subset to only fetch data for institutions known to contain depository-type accounts for the user
    institutions_w_depository = [
        inst for inst in institutions if "depository" in inst.account_types
    ]

    # Constructs a dataframe that contains transactions from all institutions for the user
    record_list = []
    for institution in institutions_w_depository:
        transactions = client.transactions.get(
            user_uuid=user_uuid,
            institution_id=institution.institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_endtime,
            account_types=["depository"],
        )
        record_list.extend([dict(transaction) for transaction in transactions])

    # if no data available for the user, return None
    if len(record_list) == 0:
        return None

    record_df = pd.DataFrame(record_list)
    credit_df = record_df[(record_df.impact == "CREDIT")]

    # if no data available for credit, return None
    if credit_df.empty:
        return None

    credit_df["date"] = pd.to_datetime(credit_df["ts"], unit="s")
    std = credit_df.set_index("date").resample("W")["amount"].sum().std()
    return std


if __name__ == "__main__":
    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = Client(token)

    now = datetime(2021, 11, 1)
    now_less_84 = now - timedelta(days=84)

    standard_deviation_of_week_to_week_sum_of_credits_0_84 = (
        get_standard_deviation_of_week_to_week_sum_of_credits(
            client=client,
            user_uuid=user_uuid,
            utc_starttime=now_less_84,
            utc_endtime=now,
        )
    )

    print(standard_deviation_of_week_to_week_sum_of_credits_0_84)
