import asyncio
from datetime import datetime, timezone
import os
from typing import Optional
from pngme.api import AsyncClient


async def get_count_user_shared_device_ids(
    api_client: AsyncClient,
    user_uuid: str,
    utc_starttime: datetime,
    utc_endtime: datetime
    ) -> Optional[int]:
    """ Returns the number of users with same device as the provided one within a time window

        i.e.: This number of users can be used to infer the number of sim swaps, as they reflect how the same
        device is used with different phone numbers.
    
    Args:
        api_client: Pngme Async API client
        user_uuid: Pngme mobile phone user_uuid
        utc_starttime: the UTC time to start the time window
        utc_endtime: the UTC time to end the time window
        
    Returns:
        Returns the number of users with same device as the provided one
            If user_uuid is not found, returns None
            
    """
   
    
    # STEP 1: Search for user with the provided user_uuid
    users = await api_client.users.get(search=user_uuid)

    # STEP 2: If user_uuid is not found, return None (wrong user uuid was passed)
    if not users:
        return None

    # STEP 3: Searching by user_uuid either returns a list of one user or an empty list
    #         As we know that the list is not empty now, we can safely take the first element
    user = users[0]
    
    # STEP 4: Search for users with the same device id 
    #         Important! The original user is included in the response
    same_device_users = await api_client.users.get(search=user["device_id"])

    # STEP 5: Return the number of users with same device id within the time window
    count = 0
    for same_device_user in same_device_users:
        if same_device_user["uuid"] == user["uuid"]:
            # As we want to provide the numer of OTHER users sharing the device id,
            # we need to exclude the original user. This also removes the complexity
            # of the original user being counted only if its updated_at is within the time window
            continue
        updated_at = datetime.fromisoformat(same_device_user["updated_at"])
        if updated_at <= utc_endtime.replace(tzinfo=timezone.utc) and updated_at >= utc_starttime.replace(tzinfo=timezone.utc):
            count += 1

    return count


if __name__ == "__main__":
    # Mercy Otingo, mercy@pngme.demo, 234112312
    # This user was last updated at 2021-11-16

    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"
    token = os.environ["PNGME_TOKEN"]

    client = AsyncClient(token)

    # Here you can set the time window to search for users with the same device id
    # We are going to search across two weeks around 2021-11-17 as we have meaningful data for
    # demo purposes in that time window

    # One alternative is to set a central timestamp and search with plus/minus a semi-window of
    # meaningful days depending on the application you are using this code for
    utc_starttime = datetime(2021, 11, 10, tzinfo=timezone.utc)
    utc_endtime = datetime(2021, 11, 24, tzinfo=timezone.utc)

    async def main():
        count_of_user_shared_device_ids = await get_count_user_shared_device_ids(
            client,
            user_uuid,
            utc_starttime,
            utc_endtime,

        )
        print(count_of_user_shared_device_ids)

    asyncio.run(main())
