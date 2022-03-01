import os

from pngme.api import Client

token = os.environ["PNGME_TOKEN"]
client = Client(token)


# Fetch the user's credit report.
user_uuid = "958a5ae8-f3a3-41d5-ae48-177fdc19e3f4"
credit_report = client.credit_report.get(user_uuid)


# Sum the values of all open and late_payment tradelines.
nondefault_tradelines = [
    *credit_report["tradelines"]["open"],
    *credit_report["tradelines"]["late_payments"],
]

nondefault_loan_balance = 0
for tradeline in nondefault_tradelines:
    amount = tradeline["amount"]
    if amount:
        nondefault_loan_balance += amount


# TODO: returns zero for demo user. There is one open tradeline but it appears
# classified incorrectly and returns amount=None :
#   "Your repayment of KES 7754.0 has been received. Your loan is fully repaid!..."
print(nondefault_loan_balance)
