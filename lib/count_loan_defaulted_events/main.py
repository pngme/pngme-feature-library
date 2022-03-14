#!/usr/bin/env python3

import os
from datetime import datetime, timedelta

from pngme.api import Client


def get_count_loan_defaulted_events(
    api_client: Client, user_uuid: str, utc_starttime: datetime, utc_endtime: datetime
) -> float:
    """
    Count events labelled with LoanDefaulted across all institutions.

    Args:
        api_client: Pngme API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the datetime for the left-hand-side of the time-window
        utc_endtime: the datetime for the right-hand-side of the time-window

    Returns:
        count of events where a user has defaulted on a loan within the given time window
    """
    label = "LoanDefaulted"

    institutions = api_client.institutions.get(user_uuid)

    count = 0
    for institution in institutions:
        overdraft_events = api_client.alerts.get(
            user_uuid=user_uuid,
            institution_id=institution.institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_endtime,
            labels=[label],
        )
        count += len(overdraft_events)

    return count


if __name__ == "__main__":
    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = Client(token)

    now = datetime(2021, 12, 1)
    now_less_30 = now - timedelta(days=30)
    now_less_60 = now - timedelta(days=60)
    now_less_90 = now - timedelta(days=90)

    count_loan_defaulted_events_0_30 = get_count_loan_defaulted_events(
        api_client=client,
        user_uuid=user_uuid,
        utc_starttime=now_less_30,
        utc_endtime=now,
    )
    count_loan_defaulted_events_31_60 = get_count_loan_defaulted_events(
        api_client=client,
        user_uuid=user_uuid,
        utc_starttime=now_less_60,
        utc_endtime=now_less_30,
    )
    count_loan_defaulted_events_61_90 = get_count_loan_defaulted_events(
        api_client=client,
        user_uuid=user_uuid,
        utc_starttime=now_less_90,
        utc_endtime=now_less_60,
    )

    print(count_loan_defaulted_events_0_30)
    print(count_loan_defaulted_events_31_60)
    print(count_loan_defaulted_events_61_90)
