import os
import json
from datetime import datetime
import snowflake.connector
import requests


def _post_pagerduty(routing_key: str, summary: str, dedup_key: str) -> None:
    url = "https://events.pagerduty.com/v2/enqueue"
    headers = {"Content-Type": "application/json"}
    payload = {
        "routing_key": routing_key,
        "event_action": "trigger",
        "payload": {
            "summary": summary,
            "severity": "warning",
            "source": "dbt-snowflake-alert",
            "timestamp": datetime.utcnow().isoformat() + "Z",
        },
        "dedup_key": dedup_key,
    }
    resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=10)
    resp.raise_for_status()


def run_daily_balance_alerts() -> None:
    """
    Check latest unprocessed day in fact_daily_balances and alert if any org's balance
    changed by more than 50% day-over-day.
    """
    pagerduty_key = os.getenv("PAGERDUTY_ROUTING_KEY")

    con = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )
    cur = con.cursor()

    # Ensure a state table to track processed dates exists
    cur.execute(
        """
        create table if not exists alert_state (
            metric varchar,
            last_processed_date date
        )
        """
    )

    # Determine the next date to process: max(date) in fact not yet processed
    cur.execute(
        """
        with max_fact as (
            select max(date) as max_date from fact_daily_balances
        ),
        state as (
            select last_processed_date from alert_state where metric = 'daily_balance_change'
        )
        select max_date
        from max_fact
        where max_date is not null
          and coalesce((select last_processed_date from state), to_date('1900-01-01')) < max_date
        """
    )
    next_row = cur.fetchone()

    if not next_row or not next_row[0]:
        print("No new dates to process for alerts.")
        cur.close(); con.close()
        return

    process_date = next_row[0]

    # Compute day-over-day percent change vs previous day
    cur.execute(
        """
        with today as (
            select organization_id, daily_balance
            from fact_daily_balances
            where date = %s
        ),
        yesterday as (
            select organization_id, daily_balance as prev_balance
            from fact_daily_balances
            where date = dateadd(day, -1, %s)
        ),
        joined as (
            select t.organization_id,
                   t.daily_balance,
                   y.prev_balance,
                   case
                     when y.prev_balance is null or y.prev_balance = 0 then null
                     else (t.daily_balance - y.prev_balance) / nullif(abs(y.prev_balance), 0)
                   end as signed_pct_change
            from today t
            left join yesterday y on t.organization_id = y.organization_id
        )
        select organization_id, daily_balance, prev_balance, signed_pct_change
        from joined
        where signed_pct_change is not null and abs(signed_pct_change) > 0.5
        order by abs(signed_pct_change) desc
        """,
        (process_date, process_date),
    )
    rows = cur.fetchall()

    if not rows:
        print(f"No alerts: no >50% balance changes for {process_date}.")
    else:
        for organization_id, bal, prev_bal, pct in rows:
            try:
                pct_val = float(pct)
            except Exception:
                pct_val = pct
            pct_str = f"{pct_val:+.1%}" if isinstance(pct_val, (float, int)) else str(pct)
            message = (
                f"Org {organization_id}: balance changed {pct_str} on {process_date} "
                f"({prev_bal} -> {bal})."
            )
            print("ALERT:", message)
            if pagerduty_key:
                dedup = f"daily_balance_change:{organization_id}:{process_date}"
                try:
                    _post_pagerduty(pagerduty_key, message, dedup)
                except Exception as exc:
                    print("PagerDuty error:", exc)

    # Update state
    cur.execute("delete from alert_state where metric = 'daily_balance_change'")
    cur.execute(
        "insert into alert_state(metric, last_processed_date) values('daily_balance_change', %s)",
        (process_date,),
    )
    con.commit()
    cur.close(); con.close()


if __name__ == "__main__":
    run_daily_balance_alerts()


