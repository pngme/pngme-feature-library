#!/usr/bin/env python3

import asyncio
import os
from datetime import datetime, timedelta
import pandas as pd  # type: ignore

from pngme.api import AsyncClient


async def get_sum_of_loan_repayments(
    api_client: AsyncClient,
    user_uuid: str,
    utc_starttime: datetime,
    utc_endtime: datetime,
) -> float:
    """
    Sum of loan repayments (i.e. credit transactions across all loan accounts) in a given period.

    No currency conversions are performed.

    Sum of loan repayments is calculated by totaling credit transactions
    across all of a user's loan accounts during the given time period.

    Args:
        api_client: Pngme Async API client
        user_uuid: the Pngme user_uuid for the mobile phone user
        utc_starttime: the UTC time to start the time window
        utc_endtime: the UTC time to end the time window

    Returns:
        the sum total of all loan repayments (i.e. credit transaction amounts across all loan accounts) over the predefined ranges.
    """
    institutions = await api_client.institutions.get(user_uuid=user_uuid)

    # subset to only fetch data for institutions known to contain loan-type accounts for the user
    institutions_w_loan = [
        inst for inst in institutions if "loan" in inst.account_types
    ]

    # Constructs a dataframe that contains transactions from all institutions for the user
    record_list = []
    inst_coroutines = [
        api_client.transactions.get(
            user_uuid=user_uuid,
            institution_id=institution.institution_id,
            utc_starttime=utc_starttime,
            utc_endtime=utc_endtime,
            account_types=["loan"],
        )
        for institution in institutions_w_loan
    ]
    r = await asyncio.gather(*inst_coroutines)
    for inst_lst in r:
        record_list.extend([dict(transaction) for transaction in inst_lst])

    # if no data available for the user, assume loan repayment is zero
    if len(record_list) == 0:
        return 0.0

    record_df = pd.DataFrame(record_list)

    time_window_filter = (
        (record_df.impact == "CREDIT")
        & (record_df.ts >= utc_starttime.timestamp())
        & (record_df.ts < utc_endtime.timestamp())
    )

    return record_df[time_window_filter].amount.sum()


if __name__ == "__main__":
    # Mercy Otieno, mercy@pngme.demo.com, 254123456789
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"

    token = os.environ["PNGME_TOKEN"]
    client = AsyncClient(token)

    utc_endtime = datetime(2021, 10, 1)
    utc_starttime = utc_endtime - timedelta(days=30)

    async def main():
        sum_of_repayments = await get_sum_of_loan_repayments(
            client,
            user_uuid,
            utc_starttime,
            utc_endtime,
        )

        print(sum_of_repayments)

    asyncio.run(main())
