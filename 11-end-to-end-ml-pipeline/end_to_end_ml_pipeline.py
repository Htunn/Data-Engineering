# Databricks notebook source
# DBTITLE 1,End-to-End ML Pipeline — Overview
# MAGIC %md
# MAGIC # End-to-End ML Pipeline: Data to Trained Model
# MAGIC
# MAGIC **Objective**: Build a complete ML pipeline — from raw data ingestion to a trained and registered model — with instructions for local development on **Mac M3 Pro** using Databricks Connect.
# MAGIC
# MAGIC ## Pipeline Stages
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart LR
# MAGIC     S1[1. Data Ingestion<br/>Delta tables] --> S2[2. Exploration<br/>& Quality Checks]
# MAGIC     S2 --> S3[3. Feature<br/>Engineering]
# MAGIC     S3 --> S4[4. Model<br/>Training]
# MAGIC     S4 --> S5[5. MLflow<br/>Tracking]
# MAGIC     S5 --> S6[6. Model<br/>Evaluation]
# MAGIC     S6 --> S7[7. Model<br/>Registry]
# MAGIC     S7 --> S8[8. Serving<br/>Reference]
# MAGIC ```
# MAGIC
# MAGIC ## Two Execution Modes
# MAGIC
# MAGIC | Mode | Where | Compute | Use Case |
# MAGIC |------|------|---------|----------|
# MAGIC | **Cloud** | Databricks workspace | Serverless / cluster | Full-scale training, distributed processing |
# MAGIC | **Local (Mac M3 Pro)** | Your laptop | Apple Silicon MPS / CPU | Development, prototyping, small datasets |
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Mac M3 Pro Setup Guide
# MAGIC %md
# MAGIC ## Mac M3 Pro — Local Development Setup
# MAGIC
# MAGIC This section covers setting up your Mac M3 Pro for Databricks development. All subsequent cells work in **both** cloud and local modes.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC ```bash
# MAGIC # 1. Install Homebrew (if not installed)
# MAGIC /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
# MAGIC
# MAGIC # 2. Install Python 3.11+ and Java 17
# MAGIC brew install python@3.11 openjdk@17
# MAGIC
# MAGIC # 3. Create a virtual environment
# MAGIC python3.11 -m venv ~/databricks-env
# MAGIC source ~/databricks-env/bin/activate
# MAGIC
# MAGIC # 4. Install Databricks Connect (Spark Connect protocol)
# MAGIC pip install databricks-connect
# MAGIC
# MAGIC # 5. Install ML libraries
# MAGIC pip install mlflow scikit-learn pandas numpy matplotlib
# MAGIC
# MAGIC # 6. (Optional) PyTorch with Apple MPS support
# MAGIC pip install torch torchvision
# MAGIC ```
# MAGIC
# MAGIC ### Configure Databricks Connect
# MAGIC
# MAGIC ```bash
# MAGIC # Authenticate with your workspace
# MAGIC databricks configure --host https://<your-workspace>.cloud.databricks.com
# MAGIC
# MAGIC # Test connection
# MAGIC python -c "from databricks.connect import DatabricksSession; spark = DatabricksSession.builder.getOrCreate(); print(spark.sql('SELECT 1').collect())"
# MAGIC ```
# MAGIC
# MAGIC ### PyTorch on Apple Silicon (M3 Pro MPS)
# MAGIC
# MAGIC ```python
# MAGIC # Verify MPS (Metal Performance Shaders) is available
# MAGIC import torch
# MAGIC print(f"MPS available: {torch.backends.mps.is_available()}")
# MAGIC print(f"MPS built: {torch.backends.mps.is_built()}")
# MAGIC
# MAGIC # Use MPS for GPU acceleration on M3 Pro
# MAGIC device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")
# MAGIC print(f"Using device: {device}")
# MAGIC ```
# MAGIC
# MAGIC ### Project Structure for Local Dev
# MAGIC
# MAGIC ```
# MAGIC ~/ml-projects/
# MAGIC ├── .env                    # Databricks connection config
# MAGIC ├── connect.py             # Databricks Connect helper
# MAGIC ├── data/                   # Local data cache
# MAGIC ├── models/                 # Saved models
# MAGIC └── mlruns/                 # Local MLflow runs
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Cell 1: Connection Helper
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 1: Connection Helper — Cloud vs Local
# MAGIC
# MAGIC This cell works in both Databricks cloud and local Mac M3 Pro via Databricks Connect.

# COMMAND ----------

import os

# Detect execution environment
IN_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

if IN_DATABRICKS:
    # Cloud mode — SparkSession is pre-configured
    print("☁️  Running in Databricks cloud")
    print(f"   Runtime: {os.environ['DATABRICKS_RUNTIME_VERSION']}")
else:
    # Local mode — connect via Databricks Connect
    print("💻 Running locally on Mac M3 Pro")
    try:
        from databricks.connect import DatabricksSession
        spark = DatabricksSession.builder.getOrCreate()
        print("   Connected to Databricks via Databricks Connect")
    except ImportError:
        print("   ⚠️  Databricks Connect not installed")
        print("   Install with: pip install databricks-connect")
    except Exception as e:
        print(f"   ⚠️  Connection error: {e}")
        print("   Run: databricks configure --host https://<workspace>.cloud.databricks.com")

# Verify Spark works
test_df = spark.sql("SELECT 1 AS test")
print(f"\n✅ Spark connection verified: {test_df.collect()[0]['test']}")

# COMMAND ----------

# DBTITLE 1,Cell 2: Data Ingestion
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 2: Data Ingestion — Create Source Tables
# MAGIC
# MAGIC Create the source data tables for our ML pipeline. This uses a synthetic dataset that can also be downloaded for local use.

# COMMAND ----------

from pyspark.sql.functions import col, rand, when, lit, current_date, datediff, expr
import random

spark.sql("CREATE CATALOG IF NOT EXISTS demo")
spark.sql("CREATE SCHEMA IF NOT EXISTS demo.ml_pipeline")

# --- Raw events table (simulates application logs) ---
spark.sql("DROP TABLE IF EXISTS demo.ml_pipeline.raw_events")

events = []
for i in range(1, 5001):
    events.append((
        i,
        random.randint(1, 500),       # user_id
        random.choice(["login", "purchase", "view", "click", "logout"]),
        round(random.uniform(0.1, 100.0), 2),  # event_value
        f"2025-{random.randint(1,9):02d}-{random.randint(1,28):02d} {random.randint(0,23):02d}:00:00",  # timestamp
        random.choice(["US", "EU", "APAC", "LATAM"]),  # region
        random.choice(["web", "mobile", "api"]),  # platform
    ))

spark.createDataFrame(events, ["event_id", "user_id", "event_type", "event_value", "event_ts", "region", "platform"]) \
    .write.format("delta").mode("overwrite").saveAsTable("demo.ml_pipeline.raw_events")

# --- Labels table (churn: 0 or 1) ---
spark.sql("DROP TABLE IF EXISTS demo.ml_pipeline.user_labels")

labels = [(uid, random.choice([0, 1])) for uid in range(1, 501)]
spark.createDataFrame(labels, ["user_id", "churn_label"]) \
    .write.format("delta").mode("overwrite").saveAsTable("demo.ml_pipeline.user_labels")

print(f"✅ Data ingested:")
print(f"   raw_events: {spark.table('demo.ml_pipeline.raw_events').count()} rows")
print(f"   user_labels: {spark.table('demo.ml_pipeline.user_labels').count()} rows")
print(f"\n💡 For local dev: export these tables to Parquet for offline use:")
print(f"   spark.table('demo.ml_pipeline.raw_events').write.parquet('data/raw_events')")

# COMMAND ----------

# DBTITLE 1,Cell 3: Data Exploration & Quality
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 3: Data Exploration & Quality Checks
# MAGIC
# MAGIC Profile the raw data and check for quality issues before feature engineering.

# COMMAND ----------

from pyspark.sql.functions import col, count, when, isNull, min as spark_min, max as spark_max

raw_df = spark.table("demo.ml_pipeline.raw_events")

# Schema and basic stats
print("📋 Schema:")
raw_df.printSchema()

print(f"\n📊 Row count: {raw_df.count()}")
print(f"   Distinct users: {raw_df.select('user_id').distinct().count()}")
print(f"   Event types: {raw_df.select('event_type').distinct().count()}")

# Null checks
print("\n🔍 Null counts per column:")
null_counts = raw_df.select([
    count(when(col(c).isNull(), c)).alias(c) for c in raw_df.columns
])
null_counts.display()

# Event type distribution
print("\n📈 Event type distribution:")
raw_df.groupBy("event_type").count().orderBy("count", ascending=False).display()

# Region distribution
print("\n🌍 Region distribution:")
raw_df.groupBy("region").count().orderBy("count", ascending=False).display()

# Label distribution
labels_df = spark.table("demo.ml_pipeline.user_labels")
print("\n🏷️  Label distribution (churn):")
labels_df.groupBy("churn_label").count().display()

# COMMAND ----------

# DBTITLE 1,Cell 4: Feature Engineering
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 4: Feature Engineering
# MAGIC
# MAGIC Transform raw events into ML-ready features using PySpark aggregations.

# COMMAND ----------

from pyspark.sql.functions import col, count, sum as spark_sum, avg, max as spark_max, min as spark_min, \
    stddev, countDistinct, when, lit, datediff, current_date, to_timestamp, hour, dayofweek

# Read raw events
raw_df = spark.table("demo.ml_pipeline.raw_events").withColumn("event_ts", to_timestamp(col("event_ts")))

# --- User-level behavioral features ---
user_features = (
    raw_df.groupBy("user_id")
    .agg(
        count("event_id").alias("total_events"),
        countDistinct("event_type").alias("event_variety"),
        countDistinct("region").alias("regions_visited"),
        countDistinct("platform").alias("platforms_used"),
        spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
        spark_sum(when(col("event_type") == "login", 1).otherwise(0)).alias("login_count"),
        spark_sum(when(col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
        spark_sum(when(col("event_type") == "click", 1).otherwise(0)).alias("click_count"),
        avg("event_value").alias("avg_event_value"),
        spark_max("event_value").alias("max_event_value"),
        stddev("event_value").alias("stddev_event_value"),
    )
    .fillna(0, ["stddev_event_value"])
)

# Derived features
user_features = (
    user_features
    .withColumn("purchase_rate", col("purchase_count") / col("total_events"))
    .withColumn("login_rate", col("login_count") / col("total_events"))
    .withColumn("click_to_view_ratio", 
        when(col("view_count") > 0, col("click_count") / col("view_count")).otherwise(0))
    .withColumn("engagement_score", 
        col("total_events") * 0.3 + col("event_variety") * 10 + col("purchase_count") * 5)
    .withColumn("is_power_user", when(col("total_events") >= 15, 1).otherwise(0))
)

# Join with labels
training_df = user_features.join(spark.table("demo.ml_pipeline.user_labels"), "user_id", "inner")

# Save feature table
spark.sql("DROP TABLE IF EXISTS demo.ml_pipeline.user_features")
training_df.write.format("delta").mode("overwrite").saveAsTable("demo.ml_pipeline.user_features")

print(f"✅ Feature engineering complete: {training_df.count()} rows, {len(training_df.columns)} columns")
print(f"   Features: {[c for c in training_df.columns if c not in ['user_id', 'churn_label']]}")

# Preview
training_df.display()

# COMMAND ----------

# DBTITLE 1,Cell 5: Train-Test Split & Preprocessing
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 5: Train-Test Split & Preprocessing
# MAGIC
# MAGIC Prepare data for model training — split, encode, scale.

# COMMAND ----------

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, classification_report, confusion_matrix
import mlflow
import mlflow.sklearn

# Set MLflow experiment
mlflow.set_experiment("/Users/martin.mystery9@gmail.com/end_to_end_churn_pipeline")

# Convert to pandas (works in both cloud and local mode)
pdf = training_df.toPandas()

# Define feature columns and label
feature_cols = [c for c in pdf.columns if c not in ['user_id', 'churn_label']]
X = pdf[feature_cols]
y = pdf['churn_label']

# Train-test split (stratified)
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# Scale features
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

print(f"✅ Data prepared:")
print(f"   Training: {len(X_train)} samples ({len(feature_cols)} features)")
print(f"   Test:     {len(X_test)} samples")
print(f"   Features: {feature_cols}")
print(f"   Churn rate (train): {y_train.mean():.2%}")
print(f"   Churn rate (test):  {y_test.mean():.2%}")

# COMMAND ----------

# DBTITLE 1,Cell 6: Model Training & MLflow Tracking
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 6: Model Training & MLflow Tracking
# MAGIC
# MAGIC Train multiple models and log everything to MLflow for comparison.

# COMMAND ----------

models = {
    "logistic_regression": LogisticRegression(max_iter=1000, random_state=42),
    "random_forest": RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42),
    "gradient_boosting": GradientBoostingClassifier(n_estimators=100, max_depth=5, random_state=42),
}

best_score = 0
best_run_id = None
best_model_name = None

for name, model in models.items():
    with mlflow.start_run(run_name=name) as run:
        # Train
        model.fit(X_train_scaled, y_train)
        y_pred = model.predict(X_test_scaled)
        y_proba = model.predict_proba(X_test_scaled)[:, 1]
        
        # Metrics
        metrics = {
            "accuracy": accuracy_score(y_test, y_pred),
            "precision": precision_score(y_test, y_pred, zero_division=0),
            "recall": recall_score(y_test, y_pred, zero_division=0),
            "f1": f1_score(y_test, y_pred, zero_division=0),
            "roc_auc": roc_auc_score(y_test, y_proba),
        }
        
        # Log params
        mlflow.log_params({"model_type": name, "n_features": len(feature_cols), "train_size": len(X_train)})
        
        # Log metrics
        mlflow.log_metrics(metrics)
        
        # Log model
        mlflow.sklearn.log_model(model, "model")
        
        # Log feature importance (if available)
        if hasattr(model, 'feature_importances_'):
            import matplotlib
            matplotlib.use('Agg')
            import matplotlib.pyplot as plt
            fig, ax = plt.subplots(figsize=(10, 6))
            sorted_idx = model.feature_importances_.argsort()
            ax.barh([feature_cols[i] for i in sorted_idx], model.feature_importances_[sorted_idx])
            ax.set_xlabel('Importance')
            ax.set_title(f'{name} — Feature Importance')
            plt.tight_layout()
            plt.savefig('/tmp/feature_importance.png', dpi=100)
            mlflow.log_artifact('/tmp/feature_importance.png')
        
        print(f"✅ {name}: ROC AUC={metrics['roc_auc']:.4f}, F1={metrics['f1']:.4f}")
        
        if metrics['roc_auc'] > best_score:
            best_score = metrics['roc_auc']
            best_run_id = run.info.run_id
            best_model_name = name

print(f"\n🏆 Best model: {best_model_name} (ROC AUC: {best_score:.4f})")
print(f"   Run ID: {best_run_id}")

# COMMAND ----------

# DBTITLE 1,Cell 7: Hyperparameter Tuning
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 7: Hyperparameter Tuning (Best Model)
# MAGIC
# MAGIC Fine-tune the best model with grid search, logging each combination.

# COMMAND ----------

from sklearn.model_selection import GridSearchCV

# Use the best model type for tuning
if best_model_name == "random_forest":
    base_model = RandomForestClassifier(random_state=42)
    param_grid = {"n_estimators": [50, 100, 200], "max_depth": [5, 10, 15], "min_samples_split": [2, 5]}
elif best_model_name == "gradient_boosting":
    base_model = GradientBoostingClassifier(random_state=42)
    param_grid = {"n_estimators": [50, 100], "max_depth": [3, 5], "learning_rate": [0.05, 0.1]}
else:
    base_model = LogisticRegression(max_iter=1000, random_state=42)
    param_grid = {"C": [0.1, 1.0, 10.0], "penalty": ["l2"]}

tuned_score = 0
tuned_run_id = None

grid = GridSearchCV(base_model, param_grid, cv=5, scoring='roc_auc', n_jobs=-1)

with mlflow.start_run(run_name=f"tuned_{best_model_name}") as run:
    grid.fit(X_train_scaled, y_train)
    y_pred = grid.predict(X_test_scaled)
    y_proba = grid.predict_proba(X_test_scaled)[:, 1]
    
    tuned_metrics = {
        "accuracy": accuracy_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred, zero_division=0),
        "recall": recall_score(y_test, y_pred, zero_division=0),
        "f1": f1_score(y_test, y_pred, zero_division=0),
        "roc_auc": roc_auc_score(y_test, y_proba),
    }
    
    mlflow.log_params({"best_params": str(grid.best_params_)})
    mlflow.log_metrics(tuned_metrics)
    mlflow.sklearn.log_model(grid.best_estimator_, "tuned_model")
    
    tuned_score = tuned_metrics["roc_auc"]
    tuned_run_id = run.info.run_id
    
    print(f"✅ Tuned {best_model_name}:")
    print(f"   Best params: {grid.best_params_}")
    print(f"   ROC AUC: {tuned_score:.4f} (baseline: {best_score:.4f})")
    print(f"   Improvement: +{(tuned_score - best_score):.4f}")

final_run_id = tuned_run_id if tuned_score > best_score else best_run_id
final_score = max(tuned_score, best_score)
print(f"\n🏆 Final best run: {final_run_id} (ROC AUC: {final_score:.4f})")

# COMMAND ----------

# DBTITLE 1,Cell 8: Model Evaluation
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 8: Model Evaluation & Confusion Matrix

# COMMAND ----------

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns

# Load best model
best_model = mlflow.sklearn.load_model(f"runs:/{final_run_id}/tuned_model")
if best_model is None:
    best_model = mlflow.sklearn.load_model(f"runs:/{final_run_id}/model")

y_pred = best_model.predict(X_test_scaled)
y_proba = best_model.predict_proba(X_test_scaled)[:, 1]

# Classification report
print("📋 Classification Report:")
print(classification_report(y_test, y_pred, target_names=['No Churn', 'Churn']))

# Confusion matrix
fig, axes = plt.subplots(1, 2, figsize=(14, 5))

cm = confusion_matrix(y_test, y_pred)
sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', ax=axes[0],
            xticklabels=['No Churn', 'Churn'], yticklabels=['No Churn', 'Churn'])
axes[0].set_title('Confusion Matrix')
axes[0].set_xlabel('Predicted')
axes[0].set_ylabel('Actual')

# ROC curve
from sklearn.metrics import roc_curve
fpr, tpr, _ = roc_curve(y_test, y_proba)
axes[1].plot(fpr, tpr, label=f'ROC (AUC = {roc_auc_score(y_test, y_proba):.4f})')
axes[1].plot([0, 1], [0, 1], 'k--', label='Random')
axes[1].set_xlabel('False Positive Rate')
axes[1].set_ylabel('True Positive Rate')
axes[1].set_title('ROC Curve')
axes[1].legend()

plt.tight_layout()
plt.savefig('/tmp/evaluation.png', dpi=100)
plt.show()

print(f"\n✅ Final model evaluation complete")
print(f"   ROC AUC: {roc_auc_score(y_test, y_proba):.4f}")

# COMMAND ----------

# DBTITLE 1,Cell 9: Model Registry
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 9: Model Registry — Register & Transition

# COMMAND ----------

from mlflow.tracking import MlflowClient

model_name = "churn_prediction_model"
model_uri = f"runs:/{final_run_id}/tuned_model"
if not spark.catalog.tableExists("demo.ml_pipeline.user_features"):
    model_uri = f"runs:/{final_run_id}/model"

# Register model
result = mlflow.register_model(model_uri=model_uri, name=model_name)
print(f"✅ Model registered: {model_name} v{result.version}")

# Transition to Staging
client = MlflowClient()
client.transition_model_version_stage(
    name=model_name, version=result.version, stage="Staging",
)
print(f"   Stage: Staging")

# Add tags
client.set_model_version_tag(
    name=model_name, version=result.version, key="pipeline", value="end-to-end",
)
client.set_model_version_tag(
    name=model_name, version=result.version, key="roc_auc", value=str(final_score),
)

print(f"\n📋 All versions:")
for v in client.search_model_versions(f"name='{model_name}'"):
    print(f"   v{v.version} | {v.current_stage} | {v.tags.get('pipeline', 'N/A')}")

# COMMAND ----------

# DBTITLE 1,Cell 10: PyTorch on Mac M3 Pro (MPS)
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 10: PyTorch on Mac M3 Pro (MPS) — Optional Deep Learning
# MAGIC
# MAGIC Train a simple neural network using PyTorch with Apple's Metal Performance Shaders (MPS)
# MAGIC for GPU acceleration on Mac M3 Pro. In Databricks cloud, this uses CUDA (if GPU cluster) or CPU.

# COMMAND ----------

try:
    import torch
    import torch.nn as nn
    from torch.utils.data import DataLoader, TensorDataset
    
    # Detect device — MPS on Mac M3 Pro, CUDA on GPU clusters, CPU otherwise
    if torch.backends.mps.is_available():
        device = torch.device("mps")
        print(f"🍎 Using Apple MPS (Metal Performance Shaders) on Mac M3 Pro")
    elif torch.cuda.is_available():
        device = torch.device("cuda")
        print(f"🟢 Using CUDA GPU")
    else:
        device = torch.device("cpu")
        print(f"⚙️  Using CPU")
    
    # Prepare data
    X_train_tensor = torch.FloatTensor(X_train_scaled).to(device)
    y_train_tensor = torch.LongTensor(y_train.values).to(device)
    X_test_tensor = torch.FloatTensor(X_test_scaled).to(device)
    y_test_tensor = torch.LongTensor(y_test.values).to(device)
    
    train_dataset = TensorDataset(X_train_tensor, y_train_tensor)
    train_loader = DataLoader(train_dataset, batch_size=64, shuffle=True)
    
    # Simple feedforward network
    class ChurnNet(nn.Module):
        def __init__(self, input_dim):
            super().__init__()
            self.net = nn.Sequential(
                nn.Linear(input_dim, 64),
                nn.ReLU(),
                nn.Dropout(0.2),
                nn.Linear(64, 32),
                nn.ReLU(),
                nn.Dropout(0.2),
                nn.Linear(32, 2),
            )
        
        def forward(self, x):
            return self.net(x)
    
    model = ChurnNet(X_train_scaled.shape[1]).to(device)
    criterion = nn.CrossEntropyLoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
    
    # Train
    with mlflow.start_run(run_name="pytorch_mps") as run:
        epochs = 50
        mlflow.log_param("model_type", "pytorch_feedforward")
        mlflow.log_param("device", str(device))
        mlflow.log_param("epochs", epochs)
        mlflow.log_param("batch_size", 64)
        
        for epoch in range(epochs):
            model.train()
            total_loss = 0
            for batch_X, batch_y in train_loader:
                optimizer.zero_grad()
                outputs = model(batch_X)
                loss = criterion(outputs, batch_y)
                loss.backward()
                optimizer.step()
                total_loss += loss.item()
            
            avg_loss = total_loss / len(train_loader)
            mlflow.log_metric("train_loss", avg_loss, step=epoch)
            
            # Evaluate every 10 epochs
            if (epoch + 1) % 10 == 0:
                model.eval()
                with torch.no_grad():
                    preds = model(X_test_tensor)
                    pred_classes = preds.argmax(dim=1).cpu().numpy()
                    acc = accuracy_score(y_test, pred_classes)
                    mlflow.log_metric("test_accuracy", acc, step=epoch)
                    print(f"   Epoch {epoch+1}/{epochs} — Loss: {avg_loss:.4f}, Acc: {acc:.4f}")
        
        # Final evaluation
        model.eval()
        with torch.no_grad():
            preds = model(X_test_tensor)
            pred_classes = preds.argmax(dim=1).cpu().numpy()
            pred_proba = torch.softmax(preds, dim=1)[:, 1].cpu().numpy()
            
            final_acc = accuracy_score(y_test, pred_classes)
            final_auc = roc_auc_score(y_test, pred_proba)
            mlflow.log_metrics({"final_accuracy": final_acc, "final_roc_auc": final_auc})
            
            print(f"\n✅ PyTorch model ({device}):")
            print(f"   Accuracy: {final_acc:.4f}")
            print(f"   ROC AUC:  {final_auc:.4f}")
            print(f"   Run ID: {run.info.run_id}")
            
            # Log model
            mlflow.pytorch.log_model(model, "pytorch_model")
    
except ImportError:
    print("⚠️  PyTorch not installed. Install with: pip install torch")
    print("   On Mac M3 Pro, PyTorch supports MPS for GPU acceleration.")

# COMMAND ----------

# DBTITLE 1,Cell 11: Model Serving & Local Inference
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 11: Model Serving & Local Inference Reference
# MAGIC
# MAGIC How to serve the model and run inference locally on Mac M3 Pro.

# COMMAND ----------

print("📋 Model Serving Options:")
print("""
1. **Real-time Serving** (Databricks cloud):
   POST /api/2.0/serving-endpoints
   {"name": "churn-endpoint", "config": {...}}

2. **Batch Scoring** (notebook or job):
   loaded_model = mlflow.sklearn.load_model(f"models:/{model_name}/Staging")
   predictions = loaded_model.predict(new_data)

3. **Local Inference on Mac M3 Pro**:
   # Download model from MLflow registry
   mlflow.artifacts.download_artifacts(
       f"runs:/{final_run_id}/tuned_model",
       dst_path="./models"
   )
   loaded = mlflow.sklearn.load_model("./models/tuned_model")
   predictions = loaded.predict(local_features)

4. **Databricks Connect — Remote Inference**:
   from databricks.connect import DatabricksSession
   spark = DatabricksSession.builder.getOrCreate()
   # Read features from Delta table, score with registered model
   features = spark.table("demo.ml_pipeline.user_features")
   # ... load model, predict, write results back
""")

print("✅ End-to-End ML Pipeline Summary:")
print("   1. Data Ingestion     → Delta tables in Unity Catalog")
print("   2. Data Exploration  → Profiled, null checks, distributions")
print("   3. Feature Engineering → 15+ behavioral features")
print("   4. Model Training    → 3 models compared via MLflow")
print("   5. Hyperparameter Tuning → Grid search on best model")
print("   6. Evaluation        → Classification report, ROC, confusion matrix")
print("   7. Model Registry    → Registered & transitioned to Staging")
print("   8. PyTorch (MPS)     → Deep learning on Mac M3 Pro GPU")
print("   9. Serving           → Real-time, batch, and local inference options")

# COMMAND ----------

# DBTITLE 1,Key Takeaways
# MAGIC %md
# MAGIC # Key Takeaways
# MAGIC
# MAGIC | Stage | Cloud (Databricks) | Local (Mac M3 Pro) |
# MAGIC |-------|--------------------|--------------------|
# MAGIC | **Data access** | Direct Delta table reads | Databricks Connect → remote Spark |
# MAGIC | **Feature engineering** | Distributed PySpark | PySpark via Connect (remote exec) |
# MAGIC | **Model training** | Serverless / GPU clusters | PyTorch with MPS (Metal) / sklearn on CPU |
# MAGIC | **Experiment tracking** | MLflow in workspace | Local MLflow → sync to workspace |
# MAGIC | **Model registry** | Unity Catalog | `mlflow.register_model()` via Connect |
# MAGIC | **Model serving** | Serverless endpoint | Local inference with downloaded model |
# MAGIC
# MAGIC ## Mac M3 Pro Tips
# MAGIC 1. **Use Databricks Connect** for Spark operations — no local Spark needed
# MAGIC 2. **PyTorch MPS** accelerates training on Apple Silicon GPU
# MAGIC 3. **Export data to Parquet** for fully offline development
# MAGIC 4. **Use `mlflow.set_tracking_uri()`** to point local MLflow to workspace
# MAGIC 5. **DAB (Declarative Automation Bundles)** can deploy from local to cloud: `databricks bundle deploy`
# MAGIC 6. **Jupyter + Databricks Connect** — set kernel to your venv with `databricks-connect` installed

# COMMAND ----------

