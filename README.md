## Financial Balance Change Alerts

### What this does
This system monitors customer account balances each day and alerts the operations team if any customer’s balance changes dramatically (more than 50%) compared to the previous day. The goal is to surface potentially risky or unexpected movements quickly so they can be reviewed and acted on.

### Where the data comes from
- Organizations: who the customers are and basic attributes (e.g., country, key dates)
- Invoices: daily financial transactions (payments, credits, refunds) recorded per customer

### How balances are calculated
- We review each customer’s transactions for a given day and convert all amounts into USD so we can compare customers consistently.
- We then apply a simple sign rule to each transaction:
  - Negative (reduces balance): paid, refund, refunded
  - Positive (increases balance): credited
  - Ignored for balance math: open, pending, awaiting_payment, processing, failed, skipped, unpayable, cancelled/canceled
- We sum the signed amounts for the day and roll them up over time to get each customer’s daily balance. This gives a consistent, cumulative view of balance by day.

### When we alert
- Every day, we compare today’s balance to yesterday’s balance for each customer.
- If the change is greater than 50% (up or down), we raise an alert for that customer.
- We only alert on new days that haven’t been checked before (so no repeated alerts for the same day).

### What you’ll see in an alert
- Customer identifier (organization_id)
- The date of the change
- Signed percent change (for example, +65% or −71%)
- The previous and current balance values

Example: “Org 123: balance changed +65.2% on 2024‑06‑20 ($10,000 → $16,520).”

### Key assumptions (currency and calculations)
- All figures are reported in USD.
- If a transaction is already in USD, we use the amount as is.
- If a transaction is in another currency, we convert to USD using the exchange rate provided with the transaction.
- Large day‑to‑day moves can be legitimate (for example, a planned large payment) or unexpected; the alert is a prompt to review, not a final judgment.

### What this means for you
- You’ll get a clear signal when a customer’s balance moves by more than 50% in a day.
- Each alert contains enough context (date, direction, magnitude) to decide whether to investigate further.

### Scope and timing
- Runs on a daily schedule.
- Looks only at the latest day of data not yet reviewed, so alerts are fresh and not repetitive.

### Tools we use (at a glance)
- dbt: transforms the raw CSV data into clean, daily balances and checks data quality.
- Snowflake: stores the data in a secure, scalable cloud database.
- Airflow: schedules the daily runs that calculate balances and check for alerts.
- PagerDuty: sends alert notifications to the operations team when large changes are detected.