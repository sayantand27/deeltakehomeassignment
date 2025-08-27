import os
import json
import logging
from datetime import datetime
import snowflake.connector
import requests
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_env_var(key: str, default: Optional[str] = None) -> str:
    """Get an environment variable or raise an error."""
    val = os.getenv(key)
    if not val and default is None:
        raise EnvironmentError(f"Missing required environment variable: {key}")
    return val or default


def _post_pagerduty(routing_key: str, summary: str, dedup_key: str) -> None:
    """Trigger a PagerDuty alert using Events API v2."""
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
    logger.info("📡 PagerDuty alert sent.")


def _post_slack(summary: str, slack_webhook_url: str) -> None:
    """Send a Slack alert to the provided webhook URL."""
    payload = {
        "text": f":warning: *{summary}*"
    }
    resp = requests.post(slack_webhook_url, data=json.dumps(payload),
                         headers={"Content-Type": "application/json"}, timeout=10)
    resp.raise_for_status()
    logger.info("📡 Slack alert sent.")


def run_daily_balance_alerts() -> None:
    """
    Checks the latest unprocessed date in `fact_daily_balances` table.
    If any organization's balance has changed by >50% compared to the previous day,
    an alert is sent via Slack and/or PagerDuty.

    Alerts are tracked in a state table (`alert_state`) to avoid reprocessing.
    """

    # Load alerting keys
    pagerduty_key = os.getenv("PAGERDUTY_ROUTING_KEY")  # Optional
    slack_url = os.getenv("SLACK_WEBHOOK_URL")          # Optional

    # Connect to Snowflake
    con = snowflake.connector.connect(
        account=get_env_var("SNOWFLAKE_ACCOUNT"),
        user=get_env_var("SNOWFLAKE_USER"),
        password=get_env_var("SNOWFLAKE_PASSWORD"),
        role=get_env_var("SNOWFLAKE_ROLE"),
        warehouse=get_env_var("SNOWFLAKE_WAREHOUSE"),
        database=get_env_var("SNOWFLAKE_DATABASE"),
        schema=get_env_var("SNOWFLAKE_SCHEMA"),
    )
    cur = con.cursor()

    # Ensure alert state table exists
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS alert_state (
            metric VARCHAR,
            last_processed_date DATE
        )
        """
    )

    # Get next unprocessed date
    cur.execute(
        """
        WITH max_fact AS (
            SELECT MAX(date) AS max_date FROM fact_daily_balances
        ),
        state AS (
            SELECT last_processed_date FROM alert_state
            WHERE metric = 'daily_balance_change'
        )
        SELECT max_date
        FROM max_fact
        WHERE max_date IS NOT NULL
          AND COALESCE((SELECT last_processed_date FROM state), TO_DATE('1900-01-01')) < max_date
        """
    )
    next_row = cur.fetchone()

    if not next_row or not next_row[0]:
        logger.info("✅ No new dates to process for alerts.")
        cur.close()
        con.close()
        return

    process_date = next_row[0]
    logger.info(f"🔍 Processing balance alerts for: {process_date}")

    # Query day-over-day balance changes
    cur.execute(
        """
        WITH today AS (
            SELECT organization_id, daily_balance
            FROM fact_daily_balances
            WHERE date = %s
        ),
        yesterday AS (
            SELECT organization_id, daily_balance AS prev_balance
            FROM fact_daily_balances
            WHERE date = DATEADD(day, -1, %s)
        ),
        joined AS (
            SELECT t.organization_id,
                   t.daily_balance,
                   y.prev_balance,
                   CASE
                       WHEN y.prev_balance IS NULL OR y.prev_balance = 0 THEN NULL
                       ELSE (t.daily_balance - y.prev_balance) / NULLIF(ABS(y.prev_balance), 0)
                   END AS signed_pct_change
            FROM today t
            LEFT JOIN yesterday y ON t.organization_id = y.organization_id
        )
        SELECT organization_id, daily_balance, prev_balance, signed_pct_change
        FROM joined
        WHERE signed_pct_change IS NOT NULL AND ABS(signed_pct_change) > 0.5
        ORDER BY ABS(signed_pct_change) DESC
        """,
        (process_date, process_date),
    )

    rows = cur.fetchall()

    if not rows:
        logger.info(f"✅ No balance changes >50% for {process_date}.")
    else:
        logger.warning(f"⚠️ Found {len(rows)} organizations with >50% balance changes.")

        for organization_id, bal, prev_bal, pct in rows:
            try:
                pct_val = float(pct)
            except Exception:
                pct_val = pct
            pct_str = f"{pct_val:+.1%}" if isinstance(pct_val, (float, int)) else str(pct)

            message = (
                f"Org {organization_id}: balance changed {pct_str} on {process_date} "
                f"({prev_bal} → {bal})"
            )
            logger.warning("ALERT: %s", message)

            dedup_key = f"daily_balance_change:{organization_id}:{process_date}"

            # Send to PagerDuty (if configured)
            if pagerduty_key:
                try:
                    _post_pagerduty(pagerduty_key, message, dedup_key)
                except Exception as exc:
                    logger.error("🚨 PagerDuty error: %s", exc)

            # Send to Slack (if configured)
            if slack_url:
                try:
                    _post_slack(message, slack_url)
                except Exception as exc:
                    logger.error("🚨 Slack error: %s", exc)

    # Update alert state
    cur.execute("DELETE FROM alert_state WHERE metric = 'daily_balance_change'")
    cur.execute(
        "INSERT INTO alert_state(metric, last_processed_date) VALUES('daily_balance_change', %s)",
        (process_date,),
    )
    con.commit()

    logger.info("✅ Alert state updated.")
    cur.close()
    con.close()


if __name__ == "__main__":
    run_daily_balance_alerts()
