#!/usr/bin/env python3
import pandas
import pendulum


def sum_of_credits(user_uuid: str, epoch_start: int, epoch_end: int) -> float:
    """
    Calculates the sum of credit-type transactions for a given user, summed across all depository accounts.
    No currency conversions are performed.
    :param user_uuid: the Pngme user_uuid for the mobile phone user
    :param epoch_start: the UTC epoch timestamp for the left-hand-side of the time-window
    :param epoch_end: the UTC epoch timestamp for the left-hand-side of the time-window
    :return: the sum total of all credit transaction amounts
    """

    return 1000.0


if __name__ == '__main__':
    import pandas as pd

    user_uuid = 'c9f0624d-4e7a-41cc-964d-9ea3b268427f'  # user: (Moses Ali, moses@pngme.demo.com, 254678901234)

    now = pendulum.now()
    now_less_30 = now.subtract(days=30)
    now_less_60 = now.subtract(days=60)
    now_less_90 = now.subtract(days=90)

    sum_of_credits_0_30 = sum_of_credits(user_uuid, now_less_30.int_timestamp, now.int_timestamp)
    sum_of_credits_31_60 = sum_of_credits(user_uuid, now_less_60.int_timestamp, now_less_60.int_timestamp)
    sum_of_credits_61_90 = sum_of_credits(user_uuid, now_less_90.int_timestamp, now_less_90.int_timestamp)

    df = pd.DataFrame({
        "user_uuid": user_uuid,
        "sum_of_credits_0_30": sum_of_credits_0_30,
        "sum_of_credits_31_60": sum_of_credits_31_60,
        "sum_of_credits_61_90": sum_of_credits_61_90,
    })

    print(df.to_markdown())
