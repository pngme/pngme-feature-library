#!/usr/bin/env python3
import asyncio
import os
from datetime import datetime, timedelta

import pandas as pd  # type: ignore
from typing import Dict, Tuple

from pngme.api import AsyncClient


async def get_count_loan_approved_or_disbursed_events(
    api_client: AsyncClient, user_uuid: str, utc_time: datetime
) -> Tuple[int, int, int]:
    """
    Count events labelled with LoanApproved or LoanDisbursed across all institutions
    over the following date ranges: last 30 days, 31-60 days and 61-90 days.

    Args:
        api_client: Pngme Async API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_time: the time-zero to use in constructing the 0-30, 31-60 and 61-90 windows

    Returns:
        count of LoanApproved or LoanDisbursed events within the given time window
    """
    labels = ["LoanApproved", "LoanDisbursed"]

    count_loan_approved_or_disbursed_events_0_30 = 0
    count_loan_approved_or_disbursed_events_31_60 = 0
    count_loan_approved_or_disbursed_events_61_90 = 0

    institutions = await api_client.institutions.get(user_uuid=user_uuid)

    utc_starttime = utc_time - timedelta(days=90)
    inst_coroutines = [
        api_client.alerts.get(
            user_uuid=user_uuid,
            institution_id=institution.institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_time,
            labels=labels,
        )
        for institution in institutions
    ]

    r = await asyncio.gather(*inst_coroutines)
    institution_ids = [institution.institution_id for institution in institutions]

    alerts_by_institution_id = dict(zip(institution_ids, r))

    alert_df_by_institution_id: Dict[str, pd.DataFrame] = {}
    for institution_id, alerts in alerts_by_institution_id.items():
        if alerts:
            alert_df_by_institution_id[institution_id] = pd.DataFrame(
                [alert.dict() for alert in alerts]
            )

    ts_30 = (utc_time - timedelta(days=30)).timestamp()
    ts_60 = (utc_time - timedelta(days=60)).timestamp()
    ts_90 = (utc_time - timedelta(days=90)).timestamp()

    for institution_id, alert_df in alert_df_by_institution_id.items():

        filter_0_30 = alert_df.ts >= ts_30
        alert_df_0_30 = alert_df[filter_0_30]

        if len(alert_df_0_30):
            count_loan_approved_or_disbursed_events_0_30 += 1

        filter_31_60 = (alert_df.ts >= ts_60) & (alert_df.ts < ts_30)
        alert_df_31_60 = alert_df[filter_31_60]

        if len(alert_df_31_60):
            count_loan_approved_or_disbursed_events_31_60 += 1

        filter_61_90 = (alert_df.ts >= ts_90) & (alert_df.ts < ts_60)
        alert_df_61_90 = alert_df[filter_61_90]

        if len(alert_df_61_90):
            count_loan_approved_or_disbursed_events_61_90 += 1

    return (
        count_loan_approved_or_disbursed_events_0_30,
        count_loan_approved_or_disbursed_events_31_60,
        count_loan_approved_or_disbursed_events_61_90,
    )


if __name__ == "__main__":
    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    now = datetime(2021, 11, 1)

    async def main():
        (
            count_loan_approved_or_disbursed_events_0_30,
            count_loan_approved_or_disbursed_events_31_60,
            count_loan_approved_or_disbursed_events_61_90,
        ) = await get_count_loan_approved_or_disbursed_events(client, user_uuid, now)

        print(count_loan_approved_or_disbursed_events_0_30)
        print(count_loan_approved_or_disbursed_events_31_60)
        print(count_loan_approved_or_disbursed_events_61_90)

    asyncio.run(main())
