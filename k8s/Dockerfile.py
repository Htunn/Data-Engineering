# ---------------------------------------------------------------------------
# Dockerfile — K8s trigger pod for Databricks AI Platform pipeline
# Pattern 1: K8s as Orchestrator, Databricks as Compute
# ---------------------------------------------------------------------------
# This image contains ONLY the trigger script + Databricks SDK.
# No Spark, no ML libraries — all compute runs on Databricks serverless.
# Image size: ~150MB (slim Python + SDK only)
# ---------------------------------------------------------------------------

FROM python:3.12-slim

# Metadata
LABEL maintainer="martin.mystery9@gmail.com"
LABEL description="K8s trigger pod for Databricks AI Platform pipeline (Pattern 1)"
LABEL pattern="K8s orchestrator → Databricks compute → OAuth M2M auth"

# Install Databricks SDK (only dependency)
# databricks-sdk >= 0.40.0 required for OAuth M2M support
RUN pip install --no-cache-dir "databricks-sdk>=0.40.0"

# Create non-root user for security
RUN useradd -m -s /bin/bash trigger
USER trigger
WORKDIR /app

# Copy the trigger script
COPY --chown=trigger:trigger trigger_pipeline.py /app/

# The script reads credentials from environment variables
# set by the K8s Secret (databricks-auth) and ConfigMap (databricks-config)
ENTRYPOINT ["python", "trigger_pipeline.py"]