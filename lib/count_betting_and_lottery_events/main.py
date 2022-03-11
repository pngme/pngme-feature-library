#!/usr/bin/env python3

import os
from datetime import datetime, timedelta

from pngme.api import Client


def get_count_betting_and_lottery_events(
    api_client: Client, user_uuid: str, utc_starttime: datetime, utc_endtime: datetime
) -> float:
    """Count events labeled with BettingAndLottery across all institutions.

    Args:
        api_client: Pngme API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the datetime for the left-hand-side of the time-window
        utc_endtime: the datetime for the right-hand-side of the time-window

    Returns:
        count of events associated with gambling behavior events within the given time window
    """
    labels = "BettingAndLottery"

    institutions = api_client.institutions.get(user_uuid=user_uuid)

    count_events = 0
    for individual_account in institutions:
        events = api_client.alerts.get(
            user_uuid=user_uuid,
            institution_id=individual_account.institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_endtime,
            labels=[labels],
        )
        count_events += len(events)

    return count_events


if __name__ == "__main__":
    # George Hassan, george@pngme.demo.com, 254789012345
    user_uuid = "6ea9480a-eafa-4eec-a1d2-f1c0c5411ccc"

    token = os.environ["PNGME_TOKEN"]
    client = Client(token)

    now = datetime(2021, 11, 1)
    now_less_90 = now - timedelta(days=90)

    count_betting_and_lettery_events_0_90 = get_count_betting_and_lottery_events(
        api_client=client,
        user_uuid=user_uuid,
        utc_starttime=now_less_90,
        utc_endtime=now,
    )

    print(count_betting_and_lettery_events_0_90)
