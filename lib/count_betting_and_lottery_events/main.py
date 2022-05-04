#!/usr/bin/env python3
import asyncio
import os
from datetime import datetime, timedelta

from pngme.api import AsyncClient

async def get_count_betting_and_lottery_events(
    api_client: AsyncClient,
    user_uuid: str,
    utc_starttime: datetime,
    utc_endtime: datetime,
) -> int:
    """
    Count events labeled with BettingAndLottery across all institutions

    Args:
        api_client: Pngme Async API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the UTC time to start the time window
        utc_endtime: the UTC time to end the time window
    Returns:
        count of BettingAndLottery events within the given time window
    """
    # STEP 1: fetch list of institutions belonging to the user
    institutions = await api_client.institutions.get(user_uuid=user_uuid)
    
    # subset to only fetch data for institutions known to contain depository-type accounts for the user
    institutions_w_depository = []
    for inst in institutions:
        if "depository" in inst.account_types:
            institutions_w_depository.append(inst)

    # STEP 2: fetch alert records for all institutions with BettingAndLottery label
    inst_coroutines = [
        api_client.alerts.get(
            user_uuid=user_uuid,
            institution_id=institution.institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_endtime,
            labels=["BettingAndLottery"],
        )
        for institution in institutions_w_depository
    ]

    r = await asyncio.gather(*inst_coroutines)

    # STEP 3: flatten alerts into a single list
    all_alerts = []
    for inst_list in r:
        all_alerts.extend([dict(alert) for alert in inst_list])

    # STEP 4: count number of alerts
    return len(all_alerts)


if __name__ == "__main__":
    # George Hassan, george@pngme.demo.com, 254789012345
    user_uuid = "7d96d780-abed-43c8-8f12-4435c9dd8ec5"

    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    utc_endtime = datetime(2021, 10, 1)
    utc_starttime = utc_endtime - timedelta(days=30)

    async def main():

        count_betting_and_lottery_events = await get_count_betting_and_lottery_events(
            client,
            user_uuid,
            utc_starttime,
            utc_endtime,
        )
        print(count_betting_and_lottery_events)

        # Dataset is set-up to return an expected value for the provided parameters
        assert count_betting_and_lottery_events == 8

    asyncio.run(main())
