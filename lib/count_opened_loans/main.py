#!/usr/bin/env python3

import os
from datetime import datetime, timedelta

from pngme.api import Client


def get_count_opened_loans(
    api_client: Client, user_uuid: str, utc_starttime: datetime, utc_endtime: datetime
) -> int:
    """Count number of unique institutions with approved or disbursed loan events.

    For a short period time, number of unique accounts in the record is a
    good approximation for approved/disbursed loan count.

    Args:
        api_client: Pngme API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: datetime for the left-hand-side of the time-window
        utc_endtime: datetime for the right-hand-side of the time-window

    Returns:
        Number of unique institutions
    """

    institutions = api_client.institutions.get(user_uuid=user_uuid)

    # Loop through account for collecting loan approval and disbursement alert info
    count_opened_loans = 0
    for institution in institutions:
        institution_id = institution.institution_id
        alerts = api_client.alerts.get(
            user_uuid=user_uuid,
            institution_id=institution_id,
            labels=["LoanApproved", "LoanDisbursed"],
            utc_starttime=utc_starttime,
            utc_endtime=utc_endtime,
        )
        if len(alerts) > 0:
            count_opened_loans += 1

    return count_opened_loans


if __name__ == "__main__":
    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = Client(token)

    now = datetime(2021, 11, 1)
    now_less_30 = now - timedelta(days=30)
    now_less_60 = now - timedelta(days=60)
    now_less_90 = now - timedelta(days=90)

    count_opened_loans_0_30 = get_count_opened_loans(
        api_client=client,
        user_uuid=user_uuid,
        utc_starttime=now_less_30,
        utc_endtime=now,
    )
    count_opened_loans_31_60 = get_count_opened_loans(
        api_client=client,
        user_uuid=user_uuid,
        utc_starttime=now_less_60,
        utc_endtime=now_less_30,
    )
    count_opened_loans_61_90 = get_count_opened_loans(
        api_client=client,
        user_uuid=user_uuid,
        utc_starttime=now_less_90,
        utc_endtime=now_less_60,
    )

    print(count_opened_loans_0_30)
    print(count_opened_loans_31_60)
    print(count_opened_loans_61_90)
