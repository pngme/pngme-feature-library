import pandas as pd
import requests

API_TOKEN = "MY_API_TOKEN"  # paste token here to run script
USER_UUID = "17683027-18f6-418f-a79c-be7090be41fd"

SECONDS_IN_ONE_DAY = 3600 * 24
HEADERS = {
    "Accept": "application/json",
    "Authorization": "Bearer {api_token}".format(api_token=API_TOKEN),
}

session = requests.Session()
session.headers.update(HEADERS)


def get_user_accounts(user_uuid: str) -> dict:
    """Get JSON object representing all accounts associated with a user
    Args:
        user_uuid (str): unique identifier of user
    Returns:
        accounts_body (dict): JSON object representing all accounts
            - accounts_body["accounts"]: list of accounts
    """

    account_url = f"""https://api.pngme.com/beta/users/{user_uuid}/accounts"""
    accounts_response = session.get(account_url)

    assert accounts_response.status_code in {200}

    accounts_body = accounts_response.json()
    return accounts_body


def _get_user_account_balances_page(
    user_uuid: str, acct_uuid: str, page: int = 1
) -> dict:
    """Get one page of the balances data object associated with a user's account
    Args:
        user_uuid (str): user identifier
        acct_uuid (str): account identifier
        page (int): page number of balances data
    Returns:
        body (dict): JSON object for specified page of balances data for a user's account
    """

    account_balance_url = f"""https://api.pngme.com/beta/users/{user_uuid}/accounts/{acct_uuid}/balances?utc_starttime=1970-01-01T00%3A00%3A00&utc_endtime=2099-12-31T23%3A59%3A59"""
    response = session.get(account_balance_url, params={"page": page})

    assert response.status_code in {200}
    body = response.json()
    return body


def get_user_balances(user_uuid: str) -> dict:
    """Get all balances for accounts of type 'depository' or 'mobilemoney'
       Returns a dictionary where the key is the acct_uuid and the value is a dataframe of balances
    Args:
        user_uuid (str): user identifier
    Returns:
        account_balances (dict): <acct_uuid, balances(pd.DataFrame)>
    """

    account_balances = dict()
    accounts = get_user_accounts(USER_UUID)

    for account in accounts:
        acct_uuid = account["acct_uuid"]
        if "depository" in account["types"] or "mobilemoney" in account["types"]:
            account_balance_df = get_user_account_balances(user_uuid, acct_uuid)
            account_balances[acct_uuid] = account_balance_df

    return account_balances


def get_user_account_balances(user_uuid: str, acct_uuid: str) -> pd.DataFrame:
    """Get a dataframe of balances for a user's account
    Args:
        user_uuid (str): user identifier
        acct_uuid (str): account identifier
    Returns:
        df (pd.DataFrame): dataframe of all observed balances for a user's account
    """
    # grab first page
    paged_bodies = []
    first_body = _get_user_account_balances_page(user_uuid, acct_uuid)
    paged_bodies.append(first_body)

    # grab subsequent pages if needed
    max_pages = first_body["_meta"]["max_pages"]
    if max_pages > 1:
        for page in range(2, max_pages + 1):
            paged_body = _get_user_account_balances_page(user_uuid, acct_uuid, page)
            paged_bodies.append(paged_body)

    # unnest objects
    records = [
        record
        for body in paged_bodies
        for account_balances in body["balances"]
        for record in account_balances["records"]
    ]

    df = pd.DataFrame(records)

    if len(df):
        df["acct_uuid"] = acct_uuid
        df.labels = df.labels.apply(set)

    return df


def _get_blank_balance_sheet(start_ts: int, end_ts: int) -> pd.DataFrame:
    """Get a blank balance sheet
        Construct a daily balance dataFrame sampling from start_ts to end_ts

    Args:
        start_ts (int): Starting timestamp of the observation window in seconds
        end_ts (int): Ending timestamp of the observation window in seconds

    Returns: pd.DataFrame
        expected columns:
            - ts: Timestamp
            - end_of_day: Filled with True value
            - balance: Filled with None

    """
    periods = range(start_ts, end_ts + 1, SECONDS_IN_ONE_DAY)
    end_of_day_balance_df = pd.DataFrame(periods, columns=["ts"])
    end_of_day_balance_df["end_of_day"] = True
    end_of_day_balance_df["balance"] = None
    return end_of_day_balance_df


def get_end_of_day_balance_sheet(
    account_uuid: str, balance_sheet: pd.Series, start_ts: int, end_ts: int
) -> pd.DataFrame:
    """Construct the end of day balance sheet

    Args:
        account_uuid (str)
        balance_sheet (pd.Series): DataFrame contains balance information
        start_ts (int): Starting timestamp of the observation window in seconds
        end_ts (int): Ending timestamp of the observation window in seconds

    Returns: pd.Series
        End of day account balance sheet
    """
    processed_balance_df = balance_sheet.copy()
    processed_balance_df["end_of_day"] = False
    blank_balance_sheet = _get_blank_balance_sheet(start_ts, end_ts)
    processed_balance_df = (
        pd.concat(
            [blank_balance_sheet, processed_balance_df], axis=0, ignore_index=True
        )
        .sort_values("ts")
        .fillna(method="ffill")
    )
    processed_balance_df = (
        processed_balance_df[processed_balance_df["end_of_day"] == True][
            ["ts", "balance"]
        ]
        .set_index("ts")
        .rename({"balance": account_uuid}, axis=1)
    )
    return processed_balance_df


def get_average_daily_balances(account_balances: dict, start_ts: int, end_ts: int):
    """Compute the daily balance

    Args:
        account_balances (dict): Output from get_user_account_balances
        start_ts (int): Starting timestamp of the observation window in seconds
        end_ts (int): Ending timestamp of the observation window in seconds

    Returns: float
    """
    end_of_day_balance_dataframes = []
    for account_uuid, account_balance_df in account_balances.items():
        if not account_balance_df.empty:
            end_of_day_balance_sheet = get_end_of_day_balance_sheet(
                account_uuid, account_balance_df, start_ts, end_ts
            )
            if end_of_day_balance_sheet.isnull().any().any():
                print(
                    f"WARNING: There are periods of time balance for {account_uuid} is unkown. Will just consider as 0 now."
                )
            end_of_day_balance_dataframes.append(end_of_day_balance_sheet)
    return pd.concat(end_of_day_balance_dataframes, axis=1).sum(axis=1).mean()


account_balances = get_user_balances(USER_UUID)
# Daily average total balance between 01/01/2022 and 02/01/2022
daily_balance = get_average_daily_balances(
    account_balances, start_ts=1640995200, end_ts=1643673600
)

print(
    f"""Daily balance between time window of 01/01/2022 and 02/01/2022 is {daily_balance}"""
)
