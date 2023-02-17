<p align="center">
  <img src="https://admin.pngme.com/logo.png" alt="Pngme" width="100" height="100">
</p>

<h3 align="center">Pngme Feature Library</h3>

<h3>🚨Depreaction Notice🚨</h3>

Pngme Feature Library is no longer supported, as of February 17, 2023. Please consider using Pngme's `/decision` APIs instead:
- [Risk score (Kenya)](https://developers.api.pngme.com/reference/getkenyadecision)
- [Risk score (Nigeria)](https://developers.api.pngme.com/reference/getnigeriadecision)

A collection of code examples which generate modelling features for a mobile phone user using [Pngme's Python Client](https://pypi.org/project/pngme-api/) for [Pngme's REST APIs](https://developers.api.pngme.com/reference/).

## Features

| Name                                                                                                            | Description                                                                                                                 |
| --------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| [average_end_of_day_depository_balance](lib/average_end_of_day_depository_balance)                              | average end-of-day balance total across all depository accounts                                                             |
| [average_end_of_day_loan_balance](lib/average_end_of_day_loan_balance)                                          | average end-of-day balance total across all loan accounts                                                                   |
| [count_betting_and_lottery_events](lib/count_betting_and_lottery_events)                                        | count of betting and lottery events across all accounts                                                                     |
| [count_insufficient_funds_events](lib/count_insufficient_funds_events)                                          | count of insufficient funds events across all accounts                                                                      |
| [count_loan_declined_events](lib/count_loan_declined_events)                                                    | count of loan declined events across all accounts                                                                           |
| [count_loan_defaulted_events](lib/count_loan_defaulted_events)                                                  | count of loan defaulted events across all accounts                                                                          |
| [count_loan_repaid_events](lib/count_loan_repaid_events)                                                        | count of loan fully repaid events across all accounts                                                                       |
| [count_loan_repayment_events](lib/count_loan_repayment_events)                                                  | count of loan repayment events across all accounts                                                                          |
| [count_missed_payment_events](lib/count_missed_payment_events)                                                  | count of missed scheduled payment events across all accounts                                                                |
| [count_opened_loans](lib/count_opened_loans)                                                                    | count of institutions with one or more opened loans over a time period                                                      |
| [count_overdraft_events](lib/count_overdraft_events)                                                            | count of overdraft events across all accounts                                                                               |
| [count_transactions_depository](lib/count_transactions_depository)                                                            | count of depository transactions across all accounts                                                                               |
| [count_user_shared_device_ids](/lib/count_user_shared_device_ids)                                               | count of user shared device ids                                                                                             |
| [daily_average_of_stacked_loan_alerts](lib/daily_average_of_stacked_loan_alerts)                                                            | daily average of open loans for a given period using labels as source                                                                               |
| [data_recency_minutes](lib/data_recency_minutes)                                                                | Returns the time in minutes between utc_endtime and the most recent financial event or alert                                |
| [debt_to_income_ratio_latest](lib/debt_to_income_ratio_latest)                                                  | ratio between the sum of balances across all loan accounts and sum of credit transactions across all depository accounts    |
| [median_end_of_day_depository_balance](lib/median_end_of_day_depository_balance)                                | median end-of-day balance total across all depository accounts                                                              |
| [net_cash_flow](lib/net_cash_flow)                                                                              | net cash flow (inflow minus outflow) across all depository accounts                                                         |
| [standard_deviation_of_week_to_week_sum_of_credits_0_84](lib/standard_deviation_of_week_to_week_sum_of_credits) | proxy for income consistency. standard deviation f week-to-week sum of credit across all depository accounts over 0-84 days |
| [sum_of_credits](lib/sum_of_credits)                                                                            | proxy for income. sum of credit transactions across all depository accounts                                                 |
| [sum_of_debits](lib/sum_of_debits)                                                                              | proxy for expense. sum of debit transactions across all depository accounts                                                 |
| [sum_of_depository_balances_latest](lib/sum_of_depository_balances_latest)                                      | current balance summed across all depository accounts                                                                       |
| [sum_of_loan_balances_latest](lib/sum_of_loan_balances_latest)                                                  | current balance summed across all loan accounts                                                                             |
| [sum_of_loan_repayments](lib/sum_of_loan_repayments)                                                            | sum of loan repayments                                                                                                      |
