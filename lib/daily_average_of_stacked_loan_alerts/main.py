#!/usr/bin/env python3
import asyncio
import math
import os
import pandas as pd

from datetime import datetime, timedelta

from pngme.api import AsyncClient

LOAN_ACTIVITY_LABELS = {
    "LoanDefaulted",
    "LoanMissedPayment",
    "LoanRepaid",
    "LoanApproved",
    "LoanDisbursed",
    "LoanRepayment",
    "LoanRepaymentReminder",
}

async def get_daily_average_of_stacked_loan_alerts(
    api_client: AsyncClient,
    user_uuid: str,
    utc_starttime: datetime,
    utc_endtime: datetime,
) -> float:
    """Daily average of open loans for a given period using labels as source

    Here, only alerts are taken into account to assess the activity of the loan, so no parsed events are used.

    This will average the amount of different loans reported for the same day
    if any loan alert is reported for that day.
    Args:
        api_client: Pngme Async API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the UTC time to start the time window
        utc_endtime: the UTC time to end the time window
    Returns:
        Daily average of open loans for a given period using labels as source
    """

    # STEP 1: fetch list of institutions belonging to the user
    institutions = await api_client.institutions.get(user_uuid=user_uuid)

    # STEP 2: get a list of all transactions for each institution
    alerts_inst_coroutines = []
    for inst in institutions:
        alerts_inst_coroutines.append(
            api_client.alerts.get(
                user_uuid=user_uuid,
                institution_id=inst["institution_id"],
                utc_starttime=utc_starttime,
                utc_endtime=utc_endtime,
            )
        )

    alerts_per_institution = await asyncio.gather(*alerts_inst_coroutines)
    alerts_flattened = []
    for institution_index, alerts in enumerate(alerts_per_institution):
        for alert in alerts:
            alert["institution_id"] = institutions[institution_index]["institution_id"]
            alerts_flattened.append(alert)

    # Keep only alerts related to loan activity
    loan_alerts = [
        a
        for a in alerts_flattened
        if set(a["labels"]).intersection(LOAN_ACTIVITY_LABELS)
    ]

    if not loan_alerts:
        return 0

    alerts_df = pd.DataFrame(loan_alerts)

    unique_institution_ids = alerts_df.institution_id.unique()
    if len(unique_institution_ids) == 1:
        return 1

    # As we want to group by day, we normalize the day index and append it to the df
    alerts_df["timestamp"] = pd.to_datetime(alerts_df["timestamp"])
    min_toc = alerts_df.timestamp.min().timestamp()
    alerts_df["day_index"] = alerts_df["timestamp"].apply(
        lambda x: math.floor(float(x.timestamp() - min_toc) / (60 * 60 * 24))
    )

    # Now we aggregate all the labels for the same day and institution
    loan_activity_per_day = (
        alerts_df.groupby(["day_index", "institution_id"])
        .size()
        .groupby("day_index")
        .size()
    )

    return float(loan_activity_per_day.mean())

if __name__ == "__main__":
    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    utc_endtime = datetime(2021, 11, 1)
    utc_starttime = utc_endtime - timedelta(days=30)

    async def main():
        avg_loan_activity_per_day = await get_daily_average_of_stacked_loan_alerts(
            client,
            user_uuid,
            utc_starttime,
            utc_endtime,
        )

        print(avg_loan_activity_per_day)

    asyncio.run(main())
