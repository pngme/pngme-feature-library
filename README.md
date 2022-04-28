<p align="center">
  <img src="https://admin.pngme.com/logo.png" alt="Pngme" width="100" height="100">
</p>

<h3 align="center">Pngme Feature Library</h3>

A collection of code examples which generate modelling features for a mobile phone user using [Pngme's Python Client](https://pypi.org/project/pngme-api/) for [Pngme's REST APIs](https://developers.api.pngme.com/reference/).

## Features

| Name                                                                                                            | Description                                                                                                                 |
| --------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| [count_betting_and_lottery_events](lib/count_betting_and_lottery_events)                                        | count of betting and lottery events across all accounts                                                                     |
| [count_insufficient_funds_events](lib/count_insufficient_funds_events)                                          | count of insufficient funds events across all accounts                                                                      |
| [count_overdraft_events](lib/count_overdraft_events)                                                            | count of overdraft events across all accounts over                                                                          |
| [count_opened_loans](lib/count_opened_loans)                                                                    | count of institutions with one or more opened loans over a time period                                                      |
| [count_loan_repayment_events](lib/count_loan_repayment_events)                                                  | count of loan repayment events across all accounts                                                                          |
| [count_missed_payment_events](lib/count_missed_payment_events)                                                  | count of missed scheduled payment events across all accounts                                                                |
| [count_loan_repaid_events](lib/count_loan_repaid_events)                                                        | count of loan fully repaid events across all accounts                                                                       |
| [count_loan_declined_events](lib/count_loan_declined_events)                                                    | count of loan declined events across all accounts                                                                           |
| [count_loan_defaulted_events](lib/count_loan_defaulted_events)                                                  | count of loan defaulted events across all accounts                                                                          |
| [sum_of_credits](lib/sum_of_credits)                                                                            | proxy for income. sum of credit transactions across all depository accounts                                                 |
| [standard_deviation_of_week_to_week_sum_of_credits_0_84](lib/standard_deviation_of_week_to_week_sum_of_credits) | proxy for income consistency. standard deviation f week-to-week sum of credit across all depository accounts over 0-84 days |
| [sum_of_debits](lib/sum_of_debits)                                                                              | proxy for expense. sum of debit transactions across all depository accounts                                                 |
| [net_cash_flow](lib/net_cash_flow)                                                                              | net cash flow (inflow minus outflow) across all depository accounts                                                         |
| [sum_of_minimum_balances](lib/sum_of_minimum_balances)                                                          | sum of minimum balances across all depository accounts                                                                      |
| [sum_of_depository_balances_latest](lib/sum_of_depository_balances_latest)                                      | current balance summed across all depository accounts                                                                       |
| [sum_of_loan_balances_latest](lib/sum_of_loan_balances_latest)                                                  | current balance summed across all loan accounts                                                                             |
| [average_of_loan_balances](lib/average_of_loan_balances)                                                        | average balance across all loan accounts                                                                                    |
| [sum_of_loan_repayments](lib/sum_of_loan_repayments)                                                            | sum of loan repayments                                                                                                      |
| [debt_to_income_ratio_latest](lib/debt_to_income_ratio_latest)                                                  | ratio between the sum of balances across all loan accounts and sum of credit transactions across all depository accounts    |
| [average_end_of_day_balance](lib/average_end_of_day_balance)                                                    | average end-of-day total balance across all depository accounts                                                             |
