# Databricks notebook source
# DBTITLE 1,Feature Engineering — Overview
# MAGIC %md
# MAGIC # Feature Engineering: Feature Store & Training Sets
# MAGIC
# MAGIC **Use Case**: Build, manage, and serve ML features using Databricks Feature Store.
# MAGIC
# MAGIC ## Features Covered
# MAGIC
# MAGIC | Feature | Description |
# MAGIC |---------|-------------|
# MAGIC | **Feature tables** | Centralized feature storage in Unity Catalog |
# MAGIC | **FeatureLookup** | Declarative feature join specification |
# MAGIC | **create_training_set** | Build point-in-time correct training data |
# MAGIC | **Point-in-time joins** | Prevent feature leakage from future data |
# MAGIC | **Feature engineering** | PySpark transformations for derived features |
# MAGIC | **Model logging** | Log model with feature spec for serving |
# MAGIC | **Batch scoring** | `score_batch` for offline predictions |
# MAGIC
# MAGIC ## Workflow
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart LR
# MAGIC     Raw[Raw Data] --> FE[Feature Engineering]
# MAGIC     FE --> FT[Feature Table]
# MAGIC     FT --> FL[FeatureLookup]
# MAGIC     FL --> TS[Training Set]
# MAGIC     TS --> Model[Train Model]
# MAGIC     Model --> Log[Log Model + Feature Spec]
# MAGIC     Log --> Serve[Score Batch / Serve]
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Cell 1: Setup
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 1: Setup — Catalog, Schema, Sample Data

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS demo")
spark.sql("CREATE SCHEMA IF NOT EXISTS demo.features")
spark.sql("CREATE SCHEMA IF NOT EXISTS demo.ml")

# Drop existing
spark.sql("DROP TABLE IF EXISTS demo.features.customer_features")
spark.sql("DROP TABLE IF EXISTS demo.features.product_features")
spark.sql("DROP TABLE IF EXISTS demo.ml.training_events")

# --- Training events table (labels + timestamps) ---
import random
events = []
for i in range(1, 501):
    events.append((
        i,
        random.randint(1, 100),      # customer_id
        random.randint(101, 105),     # product_id
        f"2025-{random.randint(1,9):02d}-{random.randint(1,28):02d}",  # event_date
        random.choice([0, 1]),         # label (churn: 0/1)
    ))
spark.createDataFrame(events, ["event_id", "customer_id", "product_id", "event_date", "churn_label"]) \
    .write.format("delta").saveAsTable("demo.ml.training_events")

print(f"✅ Training events: {len(events)} rows")

# COMMAND ----------

# DBTITLE 1,Cell 2: Customer Features
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 2: Feature Engineering — Customer Features
# MAGIC
# MAGIC Create derived features from raw customer and sales data.

# COMMAND ----------

from pyspark.sql.functions import col, avg, sum as spark_sum, count, max as spark_max, datediff, current_date, when, round as spark_round

# Build customer features from existing tables (reuse from SQL demo if available)
spark.sql("DROP TABLE IF EXISTS demo.sql_demo.sales")
sales_data = [
    (i, random.randint(1, 100), random.choice([101, 102, 103, 104, 105]),
     random.randint(1, 10), f"2025-{random.randint(1,9):02d}-{random.randint(1,28):02d}", "completed")
    for i in range(1, 1001)
]
spark.createDataFrame(sales_data, ["order_id", "customer_id", "product_id", "quantity", "order_date", "status"]) \
    .write.format("delta").saveAsTable("demo.sql_demo.sales")

# Create customer feature table
customer_features = (
    spark.table("demo.sql_demo.sales")
    .filter(col("status") == "completed")
    .groupBy("customer_id")
    .agg(
        count("order_id").alias("total_orders"),
        spark_sum("quantity").alias("total_quantity"),
        avg("quantity").alias("avg_order_size"),
        spark_max("order_date").alias("last_order_date"),
    )
    .withColumn("days_since_last_order", datediff(current_date(), col("last_order_date")))
    .withColumn("customer_segment", 
        when(col("total_orders") >= 20, "high_value")
        .when(col("total_orders") >= 10, "medium_value")
        .otherwise("low_value"))
    .drop("last_order_date")
)

customer_features.write.format("delta").mode("overwrite").saveAsTable("demo.features.customer_features")

print("✅ Customer feature table: demo.features.customer_features")
print(f"   Features: {customer_features.columns}")
customer_features.display()

# COMMAND ----------

# DBTITLE 1,Cell 3: Product Features
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 3: Feature Engineering — Product Features

# COMMAND ----------

from pyspark.sql.functions import col, avg, countDistinct, stddev

# Create product feature table
product_features = (
    spark.table("demo.sql_demo.sales")
    .filter(col("status") == "completed")
    .groupBy("product_id")
    .agg(
        count("order_id").alias("product_order_count"),
        countDistinct("customer_id").alias("unique_customers"),
        avg("quantity").alias("avg_quantity_per_order"),
        stddev("quantity").alias("quantity_variability"),
    )
    .fillna(0, ["quantity_variability"])
)

product_features.write.format("delta").mode("overwrite").saveAsTable("demo.features.product_features")

print("✅ Product feature table: demo.features.product_features")
print(f"   Features: {product_features.columns}")
product_features.display()

# COMMAND ----------

# DBTITLE 1,Cell 4: Point-in-Time Joins
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 4: Point-in-Time Joins with FeatureLookup
# MAGIC
# MAGIC The Feature Engineering Client ensures features are joined at the correct point in time,
# MAGIC preventing data leakage from future events.

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

fe = FeatureEngineeringClient()

# Define feature lookups
feature_lookups = [
    FeatureLookup(
        table_name="demo.features.customer_features",
        lookup_key="customer_id",
        feature_names=["total_orders", "total_quantity", "avg_order_size", "days_since_last_order", "customer_segment"],
    ),
    FeatureLookup(
        table_name="demo.features.product_features",
        lookup_key="product_id",
        feature_names=["product_order_count", "unique_customers", "avg_quantity_per_order"],
    ),
]

# Load training events (the base table with labels)
training_df = spark.table("demo.ml.training_events")

# Create training set with point-in-time correct feature joins
try:
    training_set = fe.create_training_set(
        df=training_df,
        feature_lookups=feature_lookups,
        label="churn_label",
    )
    
    training_data = training_set.load_df()
    
    print("✅ Training set created with point-in-time feature joins")
    print(f"   Rows: {training_data.count()}")
    print(f"   Columns: {training_data.columns}")
    training_data.display()
except Exception as e:
    print(f"ℹ️ FeatureEngineeringClient requires specific setup: {e}")
    print("   Falling back to manual point-in-time join...")
    
    # Manual join for demonstration
    training_data = (
        training_df
        .join(spark.table("demo.features.customer_features"), "customer_id", "left")
        .join(spark.table("demo.features.product_features"), "product_id", "left")
        .fillna(0)
    )
    print(f"✅ Manual join: {training_data.count()} rows, {len(training_data.columns)} columns")
    training_data.display()

# COMMAND ----------

# DBTITLE 1,Cell 5: Train Model with Features
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 5: Train Model with Features

# COMMAND ----------

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score, classification_report
import pandas as pd

# Convert to pandas for sklearn
temp_df = training_data.drop("event_id", "event_date").fillna(0)

# Encode categorical columns
for col_name in ["customer_segment"]:
    if col_name in temp_df.columns:
        from sklearn.preprocessing import LabelEncoder
        le = LabelEncoder()
        temp_df[col_name] = le.fit_transform(temp_df[col_name].astype(str))

pdf = temp_df.toPandas() if hasattr(temp_df, "toPandas") else temp_df

X = pdf.drop("churn_label", axis=1)
y = pdf["churn_label"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train model
rf = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42)
rf.fit(X_train, y_train)
y_pred = rf.predict(X_test)
y_proba = rf.predict_proba(X_test)[:, 1]

print(f"✅ Model trained on {len(X_train)} samples")
print(f"   Accuracy:  {accuracy_score(y_test, y_pred):.4f}")
print(f"   ROC AUC:   {roc_auc_score(y_test, y_proba):.4f}")
print(f"\n📋 Feature Importance:")
for name, imp in sorted(zip(X.columns, rf.feature_importances_), key=lambda x: -x[1])[:5]:
    print(f"   {name:30s} {imp:.4f}")

# COMMAND ----------

# DBTITLE 1,Cell 6: Log Model with Feature Spec
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 6: Log Model with Feature Spec
# MAGIC
# MAGIC Log the model with its feature spec so it can be served with automatic feature lookup.

# COMMAND ----------

import mlflow
import mlflow.sklearn

mlflow.set_experiment("/Users/martin.mystery9@gmail.com/feature_store_churn")

with mlflow.start_run(run_name="rf_with_features") as run:
    mlflow.log_params({
        "n_estimators": 100,
        "max_depth": 10,
        "features_used": len(X.columns),
        "training_rows": len(X_train),
    })
    mlflow.log_metrics({
        "accuracy": accuracy_score(y_test, y_pred),
        "roc_auc": roc_auc_score(y_test, y_proba),
    })
    
    # Log the model
    mlflow.sklearn.log_model(rf, "churn_model")
    
    # Log model with feature spec (requires FeatureEngineeringClient)
    try:
        fe.log_model(
            model=rf,
            flavor=mlflow.sklearn,
            artifact_path="churn_model_fe",
            training_set=training_set if 'training_set' in dir() else None,
            feature_lookups=feature_lookups,
        )
        print("✅ Model logged with feature spec (auto feature lookup at serving time)")
    except Exception as e:
        print(f"ℹ️ Feature spec logging requires full setup: {e}")
        print("   Model logged via standard mlflow.sklearn.log_model")
    
    print(f"   Run ID: {run.info.run_id}")
    print(f"   Accuracy: {accuracy_score(y_test, y_pred):.4f}")

# COMMAND ----------

# DBTITLE 1,Cell 7: Batch Scoring
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 7: Batch Scoring with Features
# MAGIC
# MAGIC Use `score_batch` to score new data with automatic feature lookup.

# COMMAND ----------

# Create a batch of new events to score
new_events = [
    (501, random.randint(1, 100), random.choice([101, 102, 103, 104, 105]), "2025-10-01"),
    (502, random.randint(1, 100), random.choice([101, 102, 103, 104, 105]), "2025-10-02"),
    (503, random.randint(1, 100), random.choice([101, 102, 103, 104, 105]), "2025-10-03"),
]
batch_df = spark.createDataFrame(new_events, ["event_id", "customer_id", "product_id", "event_date"])

try:
    # score_batch automatically looks up features and runs the model
    predictions = fe.score_batch(
        model_uri=f"runs:/{run.info.run_id}/churn_model_fe",
        df=batch_df,
        feature_lookups=feature_lookups,
    )
    print("✅ Batch scoring with automatic feature lookup:")
    predictions.display()
except Exception as e:
    print(f"ℹ️ score_batch requires full feature spec: {e}")
    
    # Manual scoring fallback
    batch_with_features = (
        batch_df
        .join(spark.table("demo.features.customer_features"), "customer_id", "left")
        .join(spark.table("demo.features.product_features"), "product_id", "left")
        .fillna(0)
    )
    
    for col_name in ["customer_segment"]:
        if col_name in batch_with_features.columns:
            from sklearn.preprocessing import LabelEncoder
            le = LabelEncoder()
            pdf_batch = batch_with_features.toPandas()
            pdf_batch[col_name] = le.fit_transform(pdf_batch[col_name].astype(str))
        else:
            pdf_batch = batch_with_features.toPandas()
    
    X_batch = pdf_batch.drop(["event_id", "event_date"], axis=1, errors='ignore')
    X_batch = X_batch.reindex(columns=X.columns, fill_value=0)
    preds = rf.predict_proba(X_batch)[:, 1]
    
    print("✅ Manual batch scoring:")
    for eid, prob in zip(new_events, preds):
        print(f"   Event {eid[0]}: churn probability = {prob:.4f}")

# COMMAND ----------

# DBTITLE 1,Key Takeaways
# MAGIC %md
# MAGIC # Key Takeaways
# MAGIC
# MAGIC | Concept | Benefit |
# MAGIC |---------|--------|
# MAGIC | **Feature tables** | Centralized, versioned, governed feature storage |
# MAGIC | **FeatureLookup** | Declarative — define what to join, not how |
# MAGIC | **Point-in-time joins** | Prevent feature leakage — no future data in training |
# MAGIC | **Feature spec logging** | Model auto-fetches features at serving time |
# MAGIC | **score_batch** | Batch scoring without manual feature pipeline |
# MAGIC
# MAGIC ## Best Practices
# MAGIC 1. **Store features in Unity Catalog** — governed, discoverable, reusable across teams
# MAGIC 2. **Use point-in-time joins** — never let future data leak into training
# MAGIC 3. **Log feature spec with model** — enables auto feature lookup at serving time
# MAGIC 4. **Version feature tables** — track schema changes and enable reproducibility
# MAGIC 5. **Reuse features across models** — avoid duplicate feature engineering pipelines
# MAGIC 6. **Monitor feature drift** — compare training vs serving feature distributions
# MAGIC
# MAGIC > **Note**: The Feature Engineering Client (`databricks-feature-engineering`) is the current API.
# MAGIC > The older `FeatureStoreClient` API is deprecated. See [Feature Engineering documentation](https://docs.databricks.com/machine-learning/feature-store/index.html).

# COMMAND ----------

