# Databricks notebook source
# DBTITLE 1,MLflow Tracking — Overview
# MAGIC %md
# MAGIC # MLflow: Model Tracking, Registry & Deployment
# MAGIC
# MAGIC **Use Case**: Track experiments, log metrics/parameters, register models, and manage the ML lifecycle.
# MAGIC
# MAGIC ## Features Covered
# MAGIC
# MAGIC | Feature | Description |
# MAGIC |---------|-------------|
# MAGIC | **Experiment tracking** | Log params, metrics, artifacts, and models |
# MAGIC | **Autologging** | Automatic logging for sklearn, xgboost, PyTorch, etc. |
# MAGIC | **Hyperparameter tuning** | Log grid/random search runs for comparison |
# MAGIC | **Model Registry** | Version, stage, and manage models centrally |
# MAGIC | **Model comparison** | Compare runs side-by-side and select best |
# MAGIC | **Model serving** | Deploy via Model Serving endpoints |
# MAGIC | **Artifact logging** | Log plots, datasets, config files |
# MAGIC
# MAGIC ## Workflow
# MAGIC
# MAGIC ```
# MAGIC   Data → Train Model → Log (params/metrics/artifacts) → Register → Serve
# MAGIC                                       ↓
# MAGIC                               Compare Runs → Select Best
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Cell 1: Setup — Experiment & Dataset
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 1: Setup — Experiment & Dataset

# COMMAND ----------

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score

# Set experiment
mlflow.set_experiment("/Users/martin.mystery9@gmail.com/customer_churn_experiment")

# Generate synthetic churn dataset
X, y = make_classification(
    n_samples=2000, n_features=15, n_informative=10, n_redundant=3,
    n_classes=2, weights=[0.7, 0.3], random_state=42
)

feature_names = [f"feature_{i}" for i in range(15)]
df = pd.DataFrame(X, columns=feature_names)
df["churn"] = y

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

print(f"✅ Dataset: {len(df)} rows, {len(feature_names)} features")
print(f"   Train: {len(X_train)}, Test: {len(X_test)}")
print(f"   Churn rate: {y.mean():.1%}")
df.display()

# COMMAND ----------

# DBTITLE 1,Cell 2: Basic MLflow Logging
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 2: Basic MLflow Logging — Manual Tracking
# MAGIC
# MAGIC Log params, metrics, and artifacts manually for full control.

# COMMAND ----------

# --- Run 1: Random Forest with default params ---
with mlflow.start_run(run_name="rf_default") as run:
    rf = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42)
    rf.fit(X_train, y_train)
    y_pred = rf.predict(X_test)
    y_proba = rf.predict_proba(X_test)[:, 1]
    
    # Log parameters
    mlflow.log_params({
        "model_type": "RandomForest",
        "n_estimators": 100,
        "max_depth": 10,
        "random_state": 42,
    })
    
    # Log metrics
    mlflow.log_metrics({
        "accuracy": accuracy_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred),
        "recall": recall_score(y_test, y_pred),
        "f1": f1_score(y_test, y_pred),
        "roc_auc": roc_auc_score(y_test, y_proba),
    })
    
    # Log the model
    mlflow.sklearn.log_model(rf, "model")
    
    # Log an artifact (feature importance plot)
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.barh(feature_names, rf.feature_importances_)
    ax.set_xlabel('Importance')
    ax.set_title('Random Forest Feature Importance')
    plt.tight_layout()
    plt.savefig('/tmp/feature_importance.png', dpi=100)
    mlflow.log_artifact('/tmp/feature_importance.png')
    
    print(f"✅ Run 1 (rf_default) logged: {run.info.run_id}")
    print(f"   Accuracy: {accuracy_score(y_test, y_pred):.4f}")
    print(f"   ROC AUC:  {roc_auc_score(y_test, y_proba):.4f}")

# --- Run 2: Logistic Regression ---
with mlflow.start_run(run_name="lr_baseline") as run:
    lr = LogisticRegression(max_iter=1000, random_state=42)
    lr.fit(X_train, y_train)
    y_pred = lr.predict(X_test)
    y_proba = lr.predict_proba(X_test)[:, 1]
    
    mlflow.log_params({"model_type": "LogisticRegression", "max_iter": 1000})
    mlflow.log_metrics({
        "accuracy": accuracy_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred),
        "recall": recall_score(y_test, y_pred),
        "f1": f1_score(y_test, y_pred),
        "roc_auc": roc_auc_score(y_test, y_proba),
    })
    mlflow.sklearn.log_model(lr, "model")
    
    print(f"✅ Run 2 (lr_baseline) logged: {run.info.run_id}")
    print(f"   Accuracy: {accuracy_score(y_test, y_pred):.4f}")
    print(f"   ROC AUC:  {roc_auc_score(y_test, y_proba):.4f}")

# COMMAND ----------

# DBTITLE 1,Cell 3: MLflow Autologging
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 3: MLflow Autologging
# MAGIC
# MAGIC `mlflow.autolog()` automatically logs params, metrics, and models for supported libraries.

# COMMAND ----------

# Enable autolog for sklearn
mlflow.autolog(log_models=True, log_input_examples=True)

with mlflow.start_run(run_name="rf_autolog") as run:
    rf = RandomForestClassifier(n_estimators=200, max_depth=15, min_samples_split=5, random_state=42)
    rf.fit(X_train, y_train)
    y_pred = rf.predict(X_test)
    
    # Autolog captures everything — but you can add custom metrics too
    mlflow.log_metric("custom_business_metric", 0.85)
    
    print(f"✅ Run 3 (rf_autolog) logged: {run.info.run_id}")
    print(f"   Autolog captured params: n_estimators=200, max_depth=15, min_samples_split=5")
    print(f"   Accuracy: {accuracy_score(y_test, y_pred):.4f}")

# COMMAND ----------

# DBTITLE 1,Cell 4: Hyperparameter Tuning
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 4: Hyperparameter Tuning with MLflow
# MAGIC
# MAGIC Run a grid search and log each combination as a separate MLflow run.

# COMMAND ----------

from itertools import product

# Define hyperparameter grid
param_grid = {
    "n_estimators": [50, 100, 200],
    "max_depth": [5, 10, 15],
    "min_samples_split": [2, 5, 10],
}

best_score = 0
best_run_id = None
best_params = None
run_count = 0

# Total combinations
total = 1
for v in param_grid.values():
    total *= len(v)
print(f"🔍 Running {total} hyperparameter combinations...")

for n_est, max_d, min_split in product(*param_grid.values()):
    run_count += 1
    with mlflow.start_run(run_name=f"hpt_run_{run_count}") as run:
        rf = RandomForestClassifier(
            n_estimators=n_est, max_depth=max_d,
            min_samples_split=min_split, random_state=42
        )
        rf.fit(X_train, y_train)
        y_pred = rf.predict(X_test)
        y_proba = rf.predict_proba(X_test)[:, 1]
        
        score = roc_auc_score(y_test, y_proba)
        
        mlflow.log_params({
            "n_estimators": n_est,
            "max_depth": max_d,
            "min_samples_split": min_split,
        })
        mlflow.log_metrics({
            "accuracy": accuracy_score(y_test, y_pred),
            "f1": f1_score(y_test, y_pred),
            "roc_auc": score,
        })
        
        if score > best_score:
            best_score = score
            best_run_id = run.info.run_id
            best_params = {"n_estimators": n_est, "max_depth": max_d, "min_samples_split": min_split}

print(f"\n🏆 Best Run: {best_run_id}")
print(f"   Best ROC AUC: {best_score:.4f}")
print(f"   Best Params: {best_params}")

# COMMAND ----------

# DBTITLE 1,Cell 5: Compare Runs
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 5: Compare Runs & Select Best Model
# MAGIC
# MAGIC Query MLflow to compare all runs and identify the best model.

# COMMAND ----------

# Search runs ordered by ROC AUC
df_runs = mlflow.search_runs(
    order_by=["metrics.roc_auc DESC"],
    max_results=10,
)

print("📊 Top 10 Runs by ROC AUC:")
display_cols = ["run_id", "metrics.roc_auc", "metrics.accuracy", "metrics.f1", 
                 "params.n_estimators", "params.max_depth", "params.min_samples_split"]
available_cols = [c for c in display_cols if c in df_runs.columns]
df_runs[available_cols].display()

print(f"\n🏆 Best model: {best_run_id}")
print(f"   ROC AUC: {best_score:.4f}")
print(f"   Params: {best_params}")

# COMMAND ----------

# DBTITLE 1,Cell 6: Model Registry
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 6: Model Registry — Register, Version & Transition
# MAGIC
# MAGIC Register the best model in MLflow Model Registry for version management.

# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient

model_name = "customer_churn_classifier"
model_uri = f"runs:/{best_run_id}/model"

# Register the model
result = mlflow.register_model(model_uri=model_uri, name=model_name)
print(f"✅ Model registered: {model_name}")
print(f"   Version: {result.version}")
print(f"   Source:  {result.source}")

# Transition to Staging
client = MlflowClient()
client.transition_model_version_stage(
    name=model_name,
    version=result.version,
    stage="Staging",
)
print(f"   Stage:   Staging")

# Add description
client.update_model_version(
    name=model_name,
    version=result.version,
    description=f"Random Forest (n_est={best_params['n_estimators']}, max_depth={best_params['max_depth']}). ROC AUC: {best_score:.4f}",
)

# List all versions
print("\n📋 All model versions:")
versions = client.search_model_versions(f"name='{model_name}'")
for v in versions:
    print(f"   Version {v.version} | Stage: {v.current_stage} | ROC AUC: {v.description}")

# COMMAND ----------

# DBTITLE 1,Cell 7: Load Registered Model & Predict
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 7: Load Registered Model & Predict
# MAGIC
# MAGIC Load a model from the registry and make predictions.

# COMMAND ----------

# Load model from registry (Staging stage)
model_uri_staging = f"models:/{model_name}/Staging"
loaded_model = mlflow.sklearn.load_model(model_uri_staging)

# Make predictions on test data
predictions = loaded_model.predict(X_test[:5])
probabilities = loaded_model.predict_proba(X_test[:5])

print(f"✅ Model loaded from registry: {model_name} (Staging)")
print(f"\n📊 Predictions on 5 test samples:")
results_df = pd.DataFrame({
    "actual": y_test[:5],
    "predicted": predictions,
    "prob_no_churn": probabilities[:, 0].round(4),
    "prob_churn": probabilities[:, 1].round(4),
})
results_df.display()

# Load specific version
model_uri_v1 = f"models:/{model_name}/1"
print(f"\n📋 Also available: {model_uri_v1}")

# COMMAND ----------

# DBTITLE 1,Cell 8: Model Serving Reference
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 8: Model Serving (Reference)
# MAGIC
# MAGIC Deploy the model as a REST endpoint for real-time inference.
# MAGIC This cell shows the commands — actual deployment requires a serving endpoint.

# COMMAND ----------

serving_config = """
🚀 Model Serving Options:

1. **Real-time Serving** (REST API):
   # Create endpoint via UI or API:
   POST /api/2.0/serving-endpoints
   {
     "name": "churn-prediction-endpoint",
     "config": {
       "served_models": [{
         "model_name": "customer_churn_classifier",
         "model_version": "1",
         "workload_size": "Small",
         "scale_to_zero_enabled": true
       }]
     }
   }

   # Call the endpoint:
   curl -X POST <endpoint-url>/invocations \\
     -H 'Content-Type: application/json' \\
     -d '{"dataframe_split": {"columns": [...], "data": [[...]]}}'

2. **Batch Inference** (Scheduled Job):
   - Load model in a notebook via mlflow.sklearn.load_model()
   - Run predictions on a Delta table
   - Write results back to Delta

3. **Streaming Inference**:
   - Load model in a foreachBatch function
   - Score streaming events in real-time

💡 Best Practice: Use Model Registry stages to control which model version is served:
   - Staging → test with a subset of traffic
   - Production → full traffic
   - Archived → keep for rollback
"""

print(serving_config)

# COMMAND ----------

# DBTITLE 1,Key Takeaways
# MAGIC %md
# MAGIC # Key Takeaways
# MAGIC
# MAGIC | Feature | Benefit |
# MAGIC |---------|--------|
# MAGIC | **Manual logging** | Full control — log custom metrics, artifacts, plots |
# MAGIC | **Autologging** | Zero-config tracking for supported libraries |
# MAGIC | **Hyperparameter tracking** | Every trial logged — compare and reproduce |
# MAGIC | **Model Registry** | Versioned models with stage transitions (Dev→Staging→Prod) |
# MAGIC | **Model comparison** | Query runs by metrics, sort, and select best |
# MAGIC | **Model serving** | Deploy as REST API, batch job, or streaming |
# MAGIC
# MAGIC ## Best Practices
# MAGIC 1. **Name experiments clearly** — include project, task, and dataset version
# MAGIC 2. **Use `mlflow.autolog()`** for rapid prototyping, manual logging for production
# MAGIC 3. **Log business metrics** alongside ML metrics — e.g., revenue impact, false positive cost
# MAGIC 4. **Register production models** — never serve directly from a run ID
# MAGIC 5. **Use stage transitions** — Staging → Production → Archived for lifecycle management
# MAGIC 6. **Tag important runs** — e.g., `champion=true`, `baseline=true` for quick filtering
# MAGIC 7. **Log input examples** — helps with model debugging and serving schema inference

# COMMAND ----------

