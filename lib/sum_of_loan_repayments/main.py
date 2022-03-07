#!/usr/bin/env python3

import os
from datetime import datetime, timedelta

from pngme.api import Client


def sum_of_loan_repayments(
    api_client: Client, user_uuid: str, utc_starttime: datetime, utc_endtime: datetime
) -> float:
    """Return the sum of loan repayments over a period for a given user

    Args:
        api_client: Pngme API client
        user_uuid: Pngme mobile phone user_uuid
        utc_starttime: the datetime for the left-hand-side of the time-window
        utc_endtime: the datetime for the right-hand-side of the time-window

    Return:
        Total number of loan repayments over a period
    """

    credit_report = api_client.credit_report.get(user_uuid)

    total_loan_repayment = 0
    for loan_repayment in credit_report["loan_repayments"]:
        loan_repayment_date = datetime.fromisoformat(loan_repayment["date"])
        if (
            loan_repayment["amount"] is not None
            and utc_starttime < loan_repayment_date <= utc_endtime
        ):
            total_loan_repayment += loan_repayment["amount"]

    return total_loan_repayment


if __name__ == "__main__":
    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = Client(token)

    decision_time = datetime(2021, 11, 1)
    decision_time_less_30 = decision_time - timedelta(days=30)
    decision_time_less_60 = decision_time - timedelta(days=60)
    decision_time_less_90 = decision_time - timedelta(days=90)

    sum_of_loan_repayments_0_30 = sum_of_loan_repayments(
        api_client=client,
        user_uuid=user_uuid,
        utc_starttime=decision_time_less_30,
        utc_endtime=decision_time,
    )
    sum_of_loan_repayments_30_60 = sum_of_loan_repayments(
        api_client=client,
        user_uuid=user_uuid,
        utc_starttime=decision_time_less_60,
        utc_endtime=decision_time_less_30,
    )
    sum_of_loan_repayments_60_90 = sum_of_loan_repayments(
        api_client=client,
        user_uuid=user_uuid,
        utc_starttime=decision_time_less_90,
        utc_endtime=decision_time_less_60,
    )

    print(sum_of_loan_repayments_0_30)
    print(sum_of_loan_repayments_30_60)
    print(sum_of_loan_repayments_60_90)
