#!/usr/bin/env python3
import asyncio
import os
from datetime import datetime, timedelta

from typing import Dict, List

from pngme.api import AsyncClient


async def get_count_loan_approved_or_disbursed_events(
    api_client: AsyncClient,
    user_uuid: str,
    utc_starttime: datetime,
    utc_endtime: datetime,
) -> int:
    """
    Count events labeled with LoanApproved or LoanDisbursed across all institutions

    Args:
        api_client: Pngme Async API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the UTC time to start the time window
        utc_endtime: the UTC time to end the time window

    Returns:
        count of LoanApproved or LoanDisbursed events within the given time window
    """
    institutions = await api_client.institutions.get(user_uuid=user_uuid)

    # subset to only fetch data for institutions known to contain loan-type accounts for the user
    institutions_w_loan = [
        inst for inst in institutions if "loan" in inst.account_types
    ]

    inst_coroutines = [
        api_client.alerts.get(
            user_uuid=user_uuid,
            institution_id=institution.institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_endtime,
            labels=["LoanApproved", "LoanDisbursed"],
        )
        for institution in institutions_w_loan
    ]

    r = await asyncio.gather(*inst_coroutines)
    institution_ids = [institution.institution_id for institution in institutions]

    alerts_by_institution_id = dict(zip(institution_ids, r))

    alerts_list_by_institution_id: Dict[str, List[Dict]] = {}
    for institution_id, alerts in alerts_by_institution_id.items():
        if alerts:
            alerts_list_by_institution_id[institution_id] = [
                alert.dict() for alert in alerts
            ]

    count_loan_approved_or_disbursed_events = 0

    for institution_id, alerts_list in alerts_list_by_institution_id.items():
        filtered_alerts_list_on_time_window = [
            alert_record
            for alert_record in alerts_list
            if (
                alert_record["ts"] >= utc_starttime.timestamp()
                and alert_record["ts"] < utc_endtime.timestamp()
            )
        ]

        if len(filtered_alerts_list_on_time_window):
            # Assumption that the user at any point of time will have a maximum
            # of 1 open loan with an institution
            count_loan_approved_or_disbursed_events += 1

    return count_loan_approved_or_disbursed_events


if __name__ == "__main__":
    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    utc_endtime = datetime(2021, 11, 1)
    utc_starttime = utc_endtime - timedelta(days=30)

    async def main():
        count_loan_approved_or_disbursed_events = (
            await get_count_loan_approved_or_disbursed_events(
                client,
                user_uuid,
                utc_starttime,
                utc_endtime,
            )
        )

        print(count_loan_approved_or_disbursed_events)

    asyncio.run(main())
