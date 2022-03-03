#!/usr/bin/env python3

import os
from datetime import datetime, timedelta

from pngme.api import Client


def count_insufficient_funds_events(
    api_client: Client, user_uuid: str, utc_starttime: datetime, utc_endtime: datetime
) -> float:
    """
    Count events labelled with InsufficientFunds across all institutions.

    Typical date ranges are last 30 days, 31-60 days and 61-90 days.

    Args:
        api_client: Pngme API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the datetime for the left-hand-side of the time-window
        utc_endtime: the datetime for the right-hand-side of the time-window

    Returns:
        count of insufficient funds events within the given time window
    """
    insufficient_funds_label = "InsufficientFunds"

    institutions = api_client.institutions.get(user_uuid=user_uuid)

    insufficient_funds_event_count = 0
    for individual_account in institutions:
        insufficient_funds_events = api_client.alerts.get(
            user_uuid=user_uuid,
            institution_id=individual_account.institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_endtime,
            labels=[insufficient_funds_label],
        )
        insufficient_funds_event_count += len(insufficient_funds_events)

    return insufficient_funds_event_count


if __name__ == "__main__":
    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"
    now = datetime(2021, 10, 1)
    now_less_30 = now - timedelta(days=30)
    now_less_60 = now - timedelta(days=60)
    now_less_90 = now - timedelta(days=90)

    token = os.environ["PNGME_TOKEN"]
    client = Client(token)

    count_insufficient_fund_events_0_30 = count_insufficient_funds_events(
        client, user_uuid, now_less_30, now
    )
    count_insufficient_fund_events_31_60 = count_insufficient_funds_events(
        client, user_uuid, now_less_60, now_less_30
    )
    count_insufficient_fund_events_61_90 = count_insufficient_funds_events(
        client, user_uuid, now_less_90, now_less_60
    )
