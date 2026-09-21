"""
trigger_pipeline.py — K8s Pod script that triggers and monitors
the Databricks AI Platform pipeline (module 21) via the Databricks SDK.

Pattern 1: K8s as Orchestrator, Databricks as Compute
- K8s CronJob runs this script on a schedule
- Script authenticates via OAuth M2M (service principal)
- Script triggers the Lakeflow Job on Databricks serverless compute
- All Spark/ML/RAG execution runs on Databricks — not in K8s
- Script polls job status until completion, then exits

Required environment variables (set via K8s Secret):
  DATABRICKS_HOST          — workspace URL (https://<workspace>.cloud.databricks.com)
  DATABRICKS_CLIENT_ID     — service principal UUID / application ID
  DATABRICKS_CLIENT_SECRET — OAuth secret for the service principal

Optional environment variables (set via K8s ConfigMap):
  DATABRICKS_JOB_NAME      — job name to trigger (default: ai-platform-pipeline)
  POLL_INTERVAL_SECONDS    — status poll interval (default: 30)
  MAX_WAIT_SECONDS         — max wait time before timeout (default: 3600)
"""

import os
import sys
import time
import logging
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("k8s-databricks-trigger")

JOB_NAME = os.environ.get("DATABRICKS_JOB_NAME", "ai-platform-pipeline")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))
MAX_WAIT = int(os.environ.get("MAX_WAIT_SECONDS", "3600"))

# ---------------------------------------------------------------------------
# Authentication — OAuth M2M via service principal
# ---------------------------------------------------------------------------

REQUIRED_ENV = [
    "DATABRICKS_HOST",
    "DATABRICKS_CLIENT_ID",
    "DATABRICKS_CLIENT_SECRET",
]

missing = [e for e in REQUIRED_ENV if not os.environ.get(e)]
if missing:
    log.error("Missing required environment variables: %s", missing)
    log.error("Set these via a K8s Secret. See k8s/README.md for details.")
    sys.exit(1)

log.info("Initializing Databricks WorkspaceClient (OAuth M2M)")
log.info("  Host: %s", os.environ["DATABRICKS_HOST"])
log.info("  Job:  %s", JOB_NAME)

w = WorkspaceClient(
    host=os.environ["DATABRICKS_HOST"],
    client_id=os.environ["DATABRICKS_CLIENT_ID"],
    client_secret=os.environ["DATABRICKS_CLIENT_SECRET"],
)

# ---------------------------------------------------------------------------
# Find the job by name
# ---------------------------------------------------------------------------

def find_job_by_name(client: WorkspaceClient, name: str) -> int:
    """Search for a Lakeflow Job by name and return its job_id."""
    for job in client.jobs.list():
        if job.settings.name == name:
            return job.job_id
    raise ValueError(f"Job '{name}' not found. Deploy it first via DAB: databricks bundle deploy -t prod")

# ---------------------------------------------------------------------------
# Trigger and monitor
# ---------------------------------------------------------------------------

def trigger_and_monitor():
    """Trigger the Databricks job and wait for completion."""

    # Step 1: Find the job
    log.info("Searching for job: %s", JOB_NAME)
    job_id = find_job_by_name(w, JOB_NAME)
    log.info("Found job_id: %s", job_id)

    # Step 2: Trigger the job run
    log.info("Triggering job run...")
    run_response = w.jobs.run_now(job_id=job_id)
    run_id = run_response.run_id
    log.info("Job triggered successfully. run_id=%s", run_id)

    # Step 3: Poll for completion
    start_time = time.time()
    log.info("Monitoring run status (poll every %ds, timeout %ds)...", POLL_INTERVAL, MAX_WAIT)

    while True:
        elapsed = time.time() - start_time
        if elapsed > MAX_WAIT:
            log.error("Timeout: run %s exceeded max wait of %ds", run_id, MAX_WAIT)
            # Cancel the run to avoid orphaned compute
            w.jobs.cancel_run(run_id=run_id)
            sys.exit(1)

        run = w.jobs.get_run(run_id=run_id)
        state = run.state
        life_cycle = state.life_cycle_state
        result_state = state.result_state

        log.info("  [%ds] life_cycle=%s, result=%s", int(elapsed), life_cycle, result_state)

        # Check terminal states
        if life_cycle == "TERMINATED":
            if result_state == "SUCCESS":
                log.info("✅ Pipeline completed successfully! run_id=%s", run_id)
                # Print run summary
                print_run_summary(run)
                sys.exit(0)
            elif result_state == "FAILED":
                log.error("❌ Pipeline FAILED! run_id=%s", run_id)
                print_run_summary(run)
                print_task_details(w, run_id)
                sys.exit(1)
            else:
                log.warning("⚠️  Pipeline terminated with result=%s", result_state)
                sys.exit(1)

        elif life_cycle == "SKIPPED":
            log.warning("⚠️  Pipeline was skipped (concurrent run policy?)")
            sys.exit(0)

        elif life_cycle in ("INTERNAL_ERROR",):
            log.error("❌ Internal error on run %s", run_id)
            sys.exit(1)

        # Still running — wait and poll again
        time.sleep(POLL_INTERVAL)


def print_run_summary(run):
    """Print a summary of the completed run."""
    log.info("--- Run Summary ---")
    log.info("  Run ID:     %s", run.run_id)
    log.info("  Job ID:     %s", run.job_id)
    log.info("  Run Name:   %s", run.run_name)
    log.info("  State:      %s", run.state.result_state)
    if run.start_time:
        start = datetime.fromtimestamp(run.start_time / 1000, tz=timezone.utc)
        log.info("  Start Time: %s", start.isoformat())
    if run.end_time:
        end = datetime.fromtimestamp(run.end_time / 1000, tz=timezone.utc)
        log.info("  End Time:   %s", end.isoformat())
        if run.start_time:
            duration = (run.end_time - run.start_time) / 1000
            log.info("  Duration:   %.1f seconds", duration)
    log.info("--------------------")


def print_task_details(client: WorkspaceClient, run_id: int):
    """Print task-level details for a failed run (for debugging)."""
    log.info("--- Task Details ---")
    try:
        run = client.jobs.get_run(run_id=run_id, include_history=False)
        if hasattr(run, "tasks") and run.tasks:
            for task in run.tasks:
                task_state = task.state.result_state if task.state else "UNKNOWN"
                log.info("  Task: %-25s  State: %s", task.task_key, task_state)
    except Exception as e:
        log.warning("Could not fetch task details: %s", e)
    log.info("--------------------")


if __name__ == "__main__":
    log.info("=== K8s → Databricks Pipeline Trigger (Pattern 1) ===")
    log.info("    K8s orchestrates · Databricks executes · OAuth M2M auth")
    log.info("")
    trigger_and_monitor()