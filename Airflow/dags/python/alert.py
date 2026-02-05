import json
import urllib.request
import urllib.error
import os
import logging

log = logging.getLogger("airflow.task")

# Read from environment variable (secure)
WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

def notify_failure(context):
    if not WEBHOOK_URL:
        log.error("DISCORD_WEBHOOK_URL not set")
        return

    ti = context["task_instance"]
    dag_id = ti.dag_id
    task_id = ti.task_id
    run_id = context["run_id"]
    log_url = ti.log_url

    payload = {
        "content": (  # <-- Discord uses "content", not "text"
            f"❌ **Airflow Task Failed**\n"
            f"**DAG:** {dag_id}\n"
            f"**Task:** {task_id}\n"
            f"**Run:** {run_id}\n"
            f"**Logs:** {log_url}"
        )
    }

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        WEBHOOK_URL,
        data=data,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "airflow-discord-webhook"
        },
        method="POST"
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            log.info("Discord alert sent. status=%s", resp.status)
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")
        log.error("Discord webhook failed. status=%s body=%s", e.code, body)