#!/usr/bin/env python3
import asyncio
import os
from datetime import datetime, timedelta

from pngme.api import AsyncClient


async def get_count_institutions_with_open_loans(
    api_client: AsyncClient,
    user_uuid: str,
    utc_starttime: datetime,
    utc_endtime: datetime,
) -> int:
    """
    Count the number of institutions where loans have been opened, in a given time window.

    Assumptions:
    Assume that an institution that has has one or more loans opened
    will have one or more LoanApproved or LoanDisbursed labels in that time window.

    Args:
        api_client: Pngme Async API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the UTC time to start the time window
        utc_endtime: the UTC time to end the time window

    Returns:
        count of institutions with one or more opened loans
    """
    # STEP 1: fetch list of institutions belonging to the user
    institutions = await api_client.institutions.get(user_uuid=user_uuid)

    # subset to only institutions that contain loan-type accounts
    institutions_w_loan = []
    for inst in institutions:
        if "loan" in inst["account_types"]:
            institutions_w_loan.append(inst)

    # STEP 2: fetch alert records for all institutions that contain loan-type accounts
    alerts_coroutines = []
    for inst_w_loan in institutions_w_loan:
        alerts_coroutines.append(
            api_client.alerts.get(
                user_uuid=user_uuid,
                institution_id=inst_w_loan["institution_id"],
                utc_starttime=utc_starttime,
                utc_endtime=utc_endtime,
                labels=["LoanApproved", "LoanDisbursed"],
            )
        )

    alerts_by_institution = await asyncio.gather(*alerts_coroutines)

    # STEP 3: count number of institutions that have 1 or more alert records with LoanApproved or LoanDisbursed label
    count_institution_with_open_loans = 0
    for alerts in alerts_by_institution:
        if len(alerts) > 0:
            count_institution_with_open_loans += 1

    return count_institution_with_open_loans


if __name__ == "__main__":
    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    utc_endtime = datetime(2021, 11, 1)
    utc_starttime = utc_endtime - timedelta(days=30)

    async def main():
        count_opened_loans = await get_count_institutions_with_open_loans(
            client,
            user_uuid,
            utc_starttime,
            utc_endtime,
        )

        print(count_opened_loans)

    asyncio.run(main())
