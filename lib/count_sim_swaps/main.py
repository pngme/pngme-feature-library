import asyncio
from datetime import datetime, timezone
import os
from typing import Optional
from pngme.api import AsyncClient


async def get_count_of_sim_swaps(
    api_client: AsyncClient,
    user_uuid: str,
    utc_timestamp: datetime,
    semi_window_days: int,
) -> Optional[int]:
    """ Returns the number of users with same device as the provided one within a time window

        This number of users can be used to infer the number of sim swaps, as they reflect how the same
        device is used with different phone numbers.
    
    Args:
        api_client: Pngme Async API client
        user_uuid: Pngme mobile phone user_uuid
        utc_timestamp: central time to search for sim swaps
        semi_window_days: devices are searched from (utc_timestamp - semi_window_days) to (utc_timestamp + semi_window_days)
        
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
    #         Important! The original user is included in the response only if its
    #         updated_at is within the time window too!
    count = 0
    for same_device_user in same_device_users:
        updated_at = datetime.fromisoformat(same_device_user["updated_at"])
        if abs((utc_timestamp - updated_at).days) <= semi_window_days:
            count += 1

    return count


if __name__ == "__main__":
    # Mercy Otingo, mercy@pngme.demo, 234112312
    
    user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"
    token = os.environ["PNGME_TOKEN"]

    client = AsyncClient(token)

    # This user was last updated at 2021-11-16
    utc_timestamp = datetime(2021, 11, 17, tzinfo=timezone.utc)

    async def main():
        count_of_sim_swaps = await get_count_of_sim_swaps(
            client,
            user_uuid,
            utc_timestamp,
            7,
        )
        print(count_of_sim_swaps)

    asyncio.run(main())
