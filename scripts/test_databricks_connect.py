#!/usr/bin/env python3
"""
Databricks Connect Test Script for Mac M3 Pro
=============================================
Tests the full data pipeline from your Mac M3 Pro to Databricks serverless compute.

Usage:
    # Make sure your venv is activated
    source ~/databricks-env/bin/activate

    # Make sure serverless env var is set
    export DATABRICKS_CONNECT_SERVERLESS=1

    # Run the test
    python3.12 scripts/test_databricks_connect.py

Or run directly with the venv Python:
    ~/databricks-env/bin/python3 scripts/test_databricks_connect.py

Prerequisites:
    1. ~/databricks-env virtual environment created
    2. pip install databricks-connect databricks-sdk mlflow scikit-learn pandas torch
    3. ~/.databrickscfg configured with host and token (NO token in this file)
    4. DATABRICKS_CONNECT_SERVERLESS=1 environment variable set

Auth config (~/.databrickscfg):
    [DEFAULT]
    host  = https://<your-workspace>.cloud.databricks.com
    token = <your-personal-access-token>
"""

import os
import sys
import time
from datetime import datetime

# ---------------------------------------------------------------------------
# 1. Environment Check
# ---------------------------------------------------------------------------

def check_environment():
    """Check that all prerequisites are met."""
    print("=" * 60)
    print("1. ENVIRONMENT CHECK")
    print("=" * 60)

    checks = []

    # Python version
    py_version = sys.version.split()[0]
    checks.append(("Python version", py_version, py_version >= "3.10"))

    # DATABRICKS_CONNECT_SERVERLESS
    serverless = os.environ.get("DATABRICKS_CONNECT_SERVERLESS", "not set")
    checks.append(("DATABRICKS_CONNECT_SERVERLESS", serverless, serverless == "1"))

    # ~/.databrickscfg exists
    cfg_path = os.path.expanduser("~/.databrickscfg")
    cfg_exists = os.path.exists(cfg_path)
    checks.append(("~/.databrickscfg exists", str(cfg_exists), cfg_exists))

    # Check required packages
    required_packages = ["databricks.connect", "databricks.sdk", "mlflow", "sklearn", "pandas", "torch"]
    for pkg in required_packages:
        try:
            __import__(pkg.split(".")[0] if "." in pkg else pkg)
            checks.append((f"Package: {pkg}", "installed", True))
        except ImportError:
            checks.append((f"Package: {pkg}", "NOT installed", False))

    # Print results
    all_passed = True
    for name, value, passed in checks:
        status = "\u2705" if passed else "\u274c"
        print(f"  {status} {name}: {value}")
        if not passed:
            all_passed = False

    if not all_passed:
        print("\n\u26a0\ufe0f  Some checks failed. Fix them before continuing.")
        print("   Run: source ~/databricks-env/bin/activate")
        print("   Run: export DATABRICKS_CONNECT_SERVERLESS=1")
        print("   Run: pip install databricks-connect databricks-sdk mlflow scikit-learn pandas torch")
        return False

    print("\n\u2705 All environment checks passed!")
    return True


# ---------------------------------------------------------------------------
# 2. Spark Connection Test
# ---------------------------------------------------------------------------

def test_spark_connection():
    """Test basic Spark connectivity to Databricks serverless."""
    print("\n" + "=" * 60)
    print("2. SPARK CONNECTION TEST")
    print("=" * 60)

    try:
        from databricks.connect import DatabricksSession

        print("  Creating Spark session via Databricks Connect (serverless)...")
        spark = DatabricksSession.builder.serverless(True).getOrCreate()
        print("  \u2705 Spark session created")

        # Test SQL
        result = spark.sql("SELECT 1 AS test_value").collect()
        assert result[0]["test_value"] == 1
        print("  \u2705 SQL query: SELECT 1 -> Row(1=1)")

        # Test current timestamp
        result = spark.sql("SELECT current_timestamp() AS ts").collect()
        print(f"  \u2705 Server timestamp: {result[0]['ts']}")

        # Test Spark version
        result = spark.sql("SELECT version() AS version").collect()
        print(f"  \u2705 Spark version: {result[0]['version']}")

        return spark
    except Exception as e:
        print(f"  \u274c Connection failed: {e}")
        print("\n  Troubleshooting:")
        print("   1. Check ~/.databrickscfg has host and token")
        print("   2. Run: export DATABRICKS_CONNECT_SERVERLESS=1")
        print("   3. Make sure you're using venv Python: ~/databricks-env/bin/python3")
        print("   4. Unset conflicting env: unset DATABRICKS_HOST")
        return None


# ---------------------------------------------------------------------------
# 3. Data Pipeline Test (Mini Medallion)
# ---------------------------------------------------------------------------

def test_mini_pipeline(spark):
    """Run a mini Bronze -> Silver -> Gold pipeline to verify end-to-end."""
    print("\n" + "=" * 60)
    print("3. MINI DATA PIPELINE TEST (Bronze -> Silver -> Gold)")
    print("=" * 60)

    catalog = "demo"
    schema = "connect_test"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    try:
        # Create catalog and schema
        print("  Creating catalog and schema...")
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        print(f"  \u2705 Catalog: {catalog}, Schema: {schema}")

        # Bronze: create raw table with sample data
        print("\n  --- BRONZE LAYER (raw data) ---")
        bronze_table = f"{catalog}.{schema}.bronze_events"
        spark.sql(f"DROP TABLE IF EXISTS {bronze_table}")

        spark.sql(f"""
            CREATE TABLE {bronze_table} AS
            SELECT 
                int(rand() * 1000) AS event_id,
                int(rand() * 100) AS user_id,
                element_at(array('login', 'purchase', 'view', 'click'), int(rand() * 4) + 1) AS event_type,
                round(rand() * 100, 2) AS event_value,
                current_timestamp() AS event_ts
            FROM range(100)
        """)
        count = spark.table(bronze_table).count()
        print(f"  \u2705 Bronze table: {bronze_table} ({count} rows)")

        # Silver: clean and transform
        print("\n  --- SILVER LAYER (cleaned) ---")
        silver_table = f"{catalog}.{schema}.silver_events"
        spark.sql(f"DROP TABLE IF EXISTS {silver_table}")

        spark.sql(f"""
            CREATE TABLE {silver_table} AS
            SELECT 
                event_id,
                user_id,
                event_type,
                event_value,
                date(event_ts) AS event_date
            FROM {bronze_table}
            WHERE event_value > 0
        """)
        count = spark.table(silver_table).count()
        print(f"  \u2705 Silver table: {silver_table} ({count} rows)")

        # Gold: aggregate
        print("\n  --- GOLD LAYER (aggregated) ---")
        gold_table = f"{catalog}.{schema}.gold_user_stats"
        spark.sql(f"DROP TABLE IF EXISTS {gold_table}")

        spark.sql(f"""
            CREATE TABLE {gold_table} AS
            SELECT
                user_id,
                count(*) AS total_events,
                sum(event_value) AS total_value,
                avg(event_value) AS avg_value,
                count(DISTINCT event_type) AS event_variety
            FROM {silver_table}
            GROUP BY user_id
            ORDER BY total_value DESC
        """)
        count = spark.table(gold_table).count()
        print(f"  \u2705 Gold table: {gold_table} ({count} rows)")

        # Show sample
        print("\n  Gold table preview (top 5):")
        spark.table(gold_table).limit(5).show(truncate=False)

        # Cleanup
        spark.sql(f"DROP TABLE IF EXISTS {bronze_table}")
        spark.sql(f"DROP TABLE IF EXISTS {silver_table}")
        spark.sql(f"DROP TABLE IF EXISTS {gold_table}")
        spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{schema}")
        print(f"\n  \u2705 Cleaned up test tables in {catalog}.{schema}")

        return True
    except Exception as e:
        print(f"  \u274c Pipeline failed: {e}")
        return False


# ---------------------------------------------------------------------------
# 4. PyTorch MPS Test (Mac M3 Pro GPU)
# ---------------------------------------------------------------------------

def test_pytorch_mps():
    """Test PyTorch with Apple Metal Performance Shaders (MPS) on Mac M3 Pro."""
    print("\n" + "=" * 60)
    print("4. PYTORCH MPS TEST (Apple Silicon GPU)")
    print("=" * 60)

    try:
        import torch

        mps_available = torch.backends.mps.is_available()
        mps_built = torch.backends.mps.is_built()

        print(f"  PyTorch version: {torch.__version__}")
        print(f"  MPS available: {mps_available}")
        print(f"  MPS built: {mps_built}")

        if mps_available:
            device = torch.device("mps")
            print(f"  \u2705 Using device: {device}")

            # Simple tensor operation on MPS
            x = torch.randn(3, 3).to(device)
            y = torch.randn(3, 3).to(device)
            z = torch.mm(x, y)
            print(f"  \u2705 Matrix multiplication on MPS: {z.shape}")
            print(f"  \u2705 Sample result: {z[0, 0].item():.4f}")
        else:
            device = torch.device("cpu")
            print(f"  \u26a0\ufe0f  MPS not available, using CPU: {device}")
            x = torch.randn(3, 3)
            y = torch.randn(3, 3)
            z = torch.mm(x, y)
            print(f"  \u2705 Matrix multiplication on CPU: {z.shape}")

        return True
    except ImportError:
        print("  \u26a0\ufe0f  PyTorch not installed")
        print("   Install with: pip install torch")
        return False
    except Exception as e:
        print(f"  \u274c PyTorch test failed: {e}")
        return False


# ---------------------------------------------------------------------------
# 5. MLflow Tracking Test
# ---------------------------------------------------------------------------

def test_mlflow():
    """Test MLflow experiment tracking (local mode)."""
    print("\n" + "=" * 60)
    print("5. MLFLOW TRACKING TEST")
    print("=" * 60)

    try:
        import mlflow
        import mlflow.sklearn
        from sklearn.linear_model import LogisticRegression
        from sklearn.datasets import make_classification

        # Set local tracking
        mlflow.set_experiment("connect-test")

        with mlflow.start_run(run_name="mac_m3pro_test") as run:
            # Log params
            mlflow.log_param("model_type", "logistic_regression")
            mlflow.log_param("device", "mac_m3_pro")

            # Train a tiny model
            X, y = make_classification(n_samples=100, n_features=5, random_state=42)
            model = LogisticRegression(max_iter=100)
            model.fit(X, y)
            accuracy = model.score(X, y)

            # Log metrics
            mlflow.log_metric("accuracy", accuracy)
            mlflow.sklearn.log_model(model, "model")

            print(f"  \u2705 MLflow run ID: {run.info.run_id}")
            print(f"  \u2705 Accuracy: {accuracy:.4f}")
            print(f"  \u2705 Model logged to MLflow")

        return True
    except Exception as e:
        print(f"  \u274c MLflow test failed: {e}")
        return False


# ---------------------------------------------------------------------------
# 6. Databricks SDK Test (API access)
# ---------------------------------------------------------------------------

def test_databricks_sdk():
    """Test Databricks SDK for workspace API access."""
    print("\n" + "=" * 60)
    print("6. DATABRICKS SDK TEST (Workspace API)")
    print("=" * 60)

    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()

        # List current user
        me = w.current_user.me()
        print(f"  \u2705 Authenticated as: {me.user_name}")

        # List catalogs
        catalogs = list(w.catalogs.list())
        print(f"  \u2705 Accessible catalogs: {len(catalogs)}")
        for c in catalogs[:5]:
            print(f"     - {c.name}")
        if len(catalogs) > 5:
            print(f"     ... and {len(catalogs) - 5} more")

        return True
    except Exception as e:
        print(f"  \u274c SDK test failed: {e}")
        print("   Make sure ~/.databrickscfg has valid credentials")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("\n")
    print("\U0001f4e6 Databricks Connect - Mac M3 Pro Pipeline Test")
    print(f"   Timestamp: {datetime.now().isoformat()}")
    print(f"   Python: {sys.executable}")
    print()

    results = {}

    # Step 1: Environment
    results["environment"] = check_environment()
    if not results["environment"]:
        print("\n\u274c Environment check failed. Fix issues above and retry.")
        sys.exit(1)

    # Step 2: Spark connection
    spark = None
    if results["environment"]:
        spark = test_spark_connection()
        results["spark"] = spark is not None

    # Step 3: Mini pipeline
    if spark:
        results["pipeline"] = test_mini_pipeline(spark)

    # Step 4: PyTorch MPS
    results["pytorch"] = test_pytorch_mps()

    # Step 5: MLflow
    results["mlflow"] = test_mlflow()

    # Step 6: Databricks SDK
    results["sdk"] = test_databricks_sdk()

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for test_name, passed in results.items():
        status = "\u2705 PASS" if passed else "\u274c FAIL"
        print(f"  {status}  {test_name}")

    total = len(results)
    passed = sum(1 for v in results.values() if v)
    print(f"\n  {passed}/{total} tests passed")

    if passed == total:
        print("\n\U0001f389 All tests passed! Your Mac M3 Pro is ready for Databricks development.")
    else:
        print("\n\u26a0\ufe0f  Some tests failed. Check the output above for details.")

    print()
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
