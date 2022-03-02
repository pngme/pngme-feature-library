#!/usr/bin/env python3
"""
Sums of overdraft events for a given user over all institutions.
Typical date ranges are last 30 days, 31-60 days and 61-90 days.
"""
from datetime import datetime
from datetime import timedelta
from pngme.api import Client

import os


def count_overdraft_events(
    api_client: Client, user_uuid: str, utc_starttime: datetime, utc_endtime: datetime
) -> float:
    """
    Calculates the sum of all events labelled with Overdraft, across all institutions.

    Args:
        api_client: Pngme API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the datetime for the left-hand-side of the time-window
        utc_endtime: the datetime for the right-hand-side of the time-window

    Returns:
        the sum of overdraft events
    """
    overdraft_label = "Overdraft"

    account_details = api_client.accounts.get(user_uuid=user_uuid)

    # collect all alert records with a label of Overdraft
    record_list = []
    for individual_account in account_details:
        overdraft_events = api_client.alerts.get(
            user_uuid=user_uuid,
            account_uuid=individual_account.acct_uuid,
            utc_starttime=utc_starttime,
            utc_endtime=utc_endtime,
            labels=[overdraft_label]
        )
        record_list.extend([dict(record) for record in overdraft_events])

    total_event_count = len(record_list)
    return total_event_count


if __name__ == "__main__":

    USER_UUID = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"  # Mercy Otieno, mercy@pngme.demo.com, 254123456789

    client = Client(access_token=os.environ["PNGME_TOKEN"])

    now = datetime(2021, 10, 1)
    now_less_30 = now - timedelta(days=30)
    now_less_60 = now - timedelta(days=60)
    now_less_90 = now - timedelta(days=90)

    count_overdraft_events_0_30 = count_overdraft_events(client, USER_UUID, now_less_30, now)
    count_overdraft_events_31_60 = count_overdraft_events(client, USER_UUID, now_less_60, now_less_30)
    count_overdraft_events_61_90 = count_overdraft_events(client, USER_UUID, now_less_90, now_less_60)
