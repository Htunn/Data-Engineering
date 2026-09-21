# K8s + Databricks Integration — Pattern 1: K8s Orchestrator

Run your Databricks AI Platform pipeline (module 21) from Kubernetes. K8s triggers and monitors the pipeline; all Spark/ML/RAG compute runs on Databricks serverless.

## Architecture

```
┌─────────────────────────┐       ┌──────────────────────────────────┐
│   K8s Cluster            │       │   Databricks Cloud                │
│                          │       │                                  │
│  ┌────────────────────┐  │       │  ┌─────────────────────────────┐ │
│  │ CronJob            │  │       │  │ Lakeflow Jobs               │ │
│  │  (daily 8AM UTC)   │  │       │  │  ┌──────────────────────┐   │ │
│  │                    │  │       │  │  │ Task 1: Ingest       │   │ │
│  │  ┌──────────────┐  │  │ OAuth │  │  │ Task 2: Silver        │   │ │
│  │  │ Pod          │──┼──┼──M2M──┼──┼──┼──▶Task 3: Gold        │   │ │
│  │  │              │  │  │       │  │  │ Task 4: ML Train     │   │ │
│  │  │ trigger_     │  │  │       │  │  │ Task 5: GenAI Prep   │   │ │
│  │  │ pipeline.py  │  │  │       │  │  │ Task 6: LLM Inference│   │ │
│  │  └──────────────┘  │  │       │  │  └──────────────────────┘   │ │
│  │                    │  │       │  │                              │ │
│  │  Secret: OAuth     │  │       │  │  Serverless Compute           │ │
│  │  ConfigMap: job    │  │       │  │  Delta Lake + UC              │ │
│  └────────────────────┘  │       │  │  MLflow + Vector Search       │ │
│                          │       │  └─────────────────────────────┘ │
│  No Spark, no ML libs    │       │   All compute runs here          │
│  Just SDK + trigger      │       │                                  │
└─────────────────────────┘       └──────────────────────────────────┘
```

## Files

| File | Purpose |
|------|---------|
| `trigger_pipeline.py` | Python script: authenticates via OAuth M2M, triggers job, polls status |
| `Dockerfile` | Builds the lightweight trigger image (Python 3.12 + databricks-sdk only) |
| `k8s-secret.yaml` | K8s Secret with OAuth credentials (host, client_id, client_secret) |
| `k8s-configmap.yaml` | K8s ConfigMap with pipeline config (job name, poll interval, timeout) |
| `k8s-cronjob.yaml` | K8s CronJob manifest (namespace, schedule, resources, security) |

## Prerequisites

### 1. Databricks Side

**a) Create a Service Principal**

1. Go to **Account Console → User Management → Service Principals → Add Service Principal**
2. Note the **UUID/Application ID** (this is your `client_id`)
3. Generate an **OAuth secret** → note the **Secret value** (this is your `client_secret`)
4. Add the service principal to your workspace

**b) Grant Unity Catalog permissions**

```sql
-- Grant access to the AI platform schemas
GRANT USE CATALOG ON CATALOG demo TO `<service-principal-uuid>`;
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA demo.ai_platform TO `<service-principal-uuid>`;

-- For MLflow experiments and model registry
GRANT EXECUTE ON CATALOG demo TO `<service-principal-uuid>`;

-- For other modules' schemas (if the pipeline touches them)
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA demo.genai TO `<service-principal-uuid>`;
```

**c) Deploy the Lakeflow Job**

The pipeline job must exist on Databricks before K8s can trigger it. Deploy it using your DAB bundle:

```bash
# Install Databricks CLI
curl -fsSL https://raw.githubusercontent.com/databricks/cli/main/scripts/install | bash

# Authenticate (use the service principal)
export DATABRICKS_HOST='https://<workspace>.cloud.databricks.com'
export DATABRICKS_CLIENT_ID='<service-principal-uuid>'
export DATABRICKS_CLIENT_SECRET='<oauth-secret>'

# Deploy the bundle (from repo root)
databricks bundle deploy -t prod
```

Or create the job manually in the Databricks UI:
- Job name: `ai-platform-pipeline`
- Tasks: see module 12 (12-jobs-orchestration-dab) for the DAG structure

### 2. K8s Side

**a) Build and push the Docker image**

```bash
cd k8s/

# Build the trigger image
docker build -t your-registry/ai-platform-trigger:latest .

# Push to your container registry
docker push your-registry/ai-platform-trigger:latest
```

**b) Create the K8s Secret** (use kubectl, not the YAML, for real credentials):

```bash
kubectl create secret generic databricks-auth \
  --from-literal=DATABRICKS_HOST='https://<workspace>.cloud.databricks.com' \
  --from-literal=DATABRICKS_CLIENT_ID='<service-principal-uuid>' \
  --from-literal=DATABRICKS_CLIENT_SECRET='<oauth-secret>' \
  -n databricks-pipeline
```

**c) Deploy the K8s resources**

```bash
# Create namespace + ConfigMap + CronJob
kubectl apply -f k8s-configmap.yaml
kubectl apply -f k8s-cronjob.yaml

# Verify
kubectl -n databricks-pipeline get cronjob
kubectl -n databricks-pipeline get pods
```

**d) Test manually** (trigger a one-off run):

```bash
kubectl -n databricks-pipeline create job --from=cronjob/ai-platform-pipeline-trigger manual-test

# Watch the logs
kubectl -n databricks-pipeline logs -f job/manual-test
```

## Security Best Practices

| Practice | How |
|----------|-----|
| **No PAT tokens** | Use OAuth M2M with service principals — tokens auto-refresh (1hr TTL) |
| **K8s Secret** | Never hardcode credentials in the image or ConfigMap |
| **External Secrets** | Use External Secrets Operator + Vault/AWS Secrets Manager in production |
| **Non-root user** | Dockerfile runs as user `trigger` (UID 1000) |
| **Read-only FS** | CronJob sets `readOnlyRootFilesystem: true` |
| **Minimal RBAC** | The ServiceAccount only needs `jobs:run` permissions |
| **Network policy** | Restrict egress to `*.databricks.com:443` only |

## Troubleshooting

| Issue | Cause | Fix |
|------|-------|-----|
| `Job 'ai-platform-pipeline' not found` | Job not deployed on Databricks | Run `databricks bundle deploy -t prod` |
| `401 Unauthorized` | Wrong OAuth credentials | Verify `client_id` and `client_secret` in the K8s Secret |
| `403 Forbidden` | SP lacks permissions | Run the GRANT statements in prerequisites |
| `Timeout after 3600s` | Pipeline runs longer than MAX_WAIT_SECONDS | Increase `MAX_WAIT_SECONDS` in ConfigMap |
| `ImagePullBackOff` | Wrong image name/registry | Update image in k8s-cronjob.yaml + ensure `imagePullSecrets` |
| Pod OOM killed | Resource limits too low | Increase memory limit in CronJob manifest |

## Monitoring

```bash
# View CronJob schedule and last run
kubectl -n databricks-pipeline get cronjob ai-platform-pipeline-trigger

# View recent jobs
kubectl -n databricks-pipeline get jobs --sort-by=.metadata.creationTimestamp

# View pod logs
kubectl -n databricks-pipeline logs -l app.kubernetes.io/name=ai-platform-trigger

# Also monitor on Databricks side:
# Jobs UI → Runs → ai-platform-pipeline
```

## Alternative Patterns

This implementation uses **Pattern 1** (K8s orchestrator → Databricks compute). For other patterns:

| Pattern | When to Use | Docs |
|---------|-----------|------|
| **1. K8s Orchestrator** (this) | K8s triggers Databricks jobs via SDK | You are here |
| **2. Databricks Connect** | K8s pod runs PySpark on Databricks serverless | [Databricks Connect](https://docs.databricks.com/aws/en/dev-tools/databricks-connect/python/tutorial-serverless/) |
| **3. DAB + GitOps** | ArgoCD/Flux deploys DAB bundles to Databricks | [DAB CI/CD](https://docs.databricks.com/aws/en/dev-tools/ci-cd/index/) |

See [docs/k8s-databricks-integration.md](../docs/k8s-databricks-integration.md) for all 3 patterns and full architecture details.

## References

- [Databricks OAuth M2M](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m/)
- [Databricks SDK for Python](https://docs.databricks.com/aws/en/dev-tools/sdk-python/)
- [Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/index/)
- [Lakeflow Jobs](https://docs.databricks.com/aws/en/jobs/)
- [CI/CD on Databricks](https://docs.databricks.com/aws/en/dev-tools/ci-cd/index/)