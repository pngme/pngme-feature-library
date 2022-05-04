#!/usr/bin/env python3
import asyncio
import os
from datetime import datetime, timedelta

from pngme.api import AsyncClient


async def get_count_insufficient_funds_events(
    api_client: AsyncClient,
    user_uuid: str,
    utc_starttime: datetime,
    utc_endtime: datetime,
) -> int:
    """
    Count events labeled with InsufficientFunds across all institutions

    Args:
        api_client: Pngme Async API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the UTC time to start the time window
        utc_endtime: the UTC time to end the time window
    Returns:
        count of InsufficientFunds events within the given time window
    """
    # STEP 1: fetch list of institutions belonging to the user
    institutions = await api_client.institutions.get(user_uuid=user_uuid)

    # subset to only fetch data for institutions known to contain depository-type accounts for the user
    institutions_w_depository = []
    for inst in institutions:
        if "depository" in inst.account_types:
            institutions_w_depository.append(inst)

    # STEP 2: fetch alert records for all institutions with InsufficientFunds events
    alerts_coroutines = []
    for inst_w_loan in institutions_w_depository:
        alerts_coroutines.append(
            api_client.alerts.get(
                user_uuid=user_uuid,
                institution_id=inst_w_loan.institution_id,
                utc_starttime=utc_starttime,
                utc_endtime=utc_endtime,
                labels=["InsufficientFunds"],
            )
        )

    alerts_per_institution = await asyncio.gather(*alerts_coroutines)

    # STEP 3: flatten alerts into a single list
    all_alerts = []
    for alerts_list in alerts_per_institution:
        for alert in alerts_list:
            all_alerts.append(alert)

    # STEP 4: count number of alerts
    return len(all_alerts)


if __name__ == "__main__":
    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    utc_endtime = datetime(2021, 10, 1)
    utc_starttime = utc_endtime - timedelta(days=30)

    async def main():
        count_insufficient_funds_events = await get_count_insufficient_funds_events(
            client,
            user_uuid,
            utc_starttime,
            utc_endtime,
        )
        print(count_insufficient_funds_events)
        
        # Dataset is set-up to return an expected value for the provided parameters
        assert count_insufficient_funds_events == 6
    asyncio.run(main())
