# Databricks notebook source
# DBTITLE 1,Overview & Setup
# Databricks notebook source
# MAGIC %md
# MAGIC # Cost Optimization and FinOps on Databricks
# MAGIC
# MAGIC **Use Case**: Monitor, analyze, and reduce Databricks spending.
# MAGIC
# MAGIC ## Features Covered
# MAGIC
# MAGIC | Feature | Description |
# MAGIC |---------|-------------|
# MAGIC | **system.billing** | DBU consumption by workspace, compute type, job, custom tag |
# MAGIC | **Compute policies** | Control who can create what compute types and sizes |
# MAGIC | **Budgeting** | Set spending budgets and alerts per team/project |
# MAGIC | **Tagging** | Attribute costs to business units, projects, environments |
# MAGIC | **Serverless vs Classic** | Cost comparison and workload migration strategies |
# MAGIC | **Right-sizing** | Match compute size to workload requirements |
# MAGIC | **Photon** | Vectorized execution engine for faster SQL/Spark |
# MAGIC | **Auto-termination** | Reduce idle compute costs |

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS demo.cost")
print("Schema demo.cost created")

# COMMAND ----------

# DBTITLE 1,Cell 2: Billing Queries
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 2: System Billing Queries
# MAGIC
# MAGIC The `system.billing.usage` table records all DBU consumption. Query it to understand spending patterns.

# COMMAND ----------

# Check if system.billing is accessible
try:
    billing_count = spark.sql("SELECT count(*) FROM system.billing.usage LIMIT 1").collect()[0][0]
    print(f"system.billing.usage accessible. Sample rows available.")
    
    # DBU consumption by SKU (compute type)
    print("\nDBU consumption by SKU (last 30 days):")
    spark.sql("""
        SELECT
            sku_name,
            sum(usage_quantity) AS total_dbus,
            round(sum(usage_quantity * usage_unit_price), 2) AS estimated_cost_usd
        FROM system.billing.usage
        WHERE usage_date >= date_add(current_date(), -30)
        GROUP BY sku_name
        ORDER BY total_dbus DESC
        LIMIT 10
    """).display()
    
except Exception as e:
    print(f"system.billing not accessible: {str(e)[:100]}")
    print("\nShowing example queries instead:")
    
    # Show example queries
    print("\n1. DBU by SKU (compute type):")
    print("   SELECT sku_name, sum(usage_quantity) AS dbus FROM system.billing.usage")
    print("   WHERE usage_date >= date_add(current_date(), -30)")
    print("   GROUP BY sku_name ORDER BY dbus DESC")
    
    print("\n2. Cost by custom tag (team/project):")
    print("   SELECT custom_tags.team, sum(usage_quantity) AS dbus")
    print("   FROM system.billing.usage LATERAL VIEW explode(custom_tags) AS custom_tags")
    print("   GROUP BY custom_tags.team")
    
    print("\n3. Daily spending trend:")
    print("   SELECT usage_date, sum(usage_quantity) AS daily_dbus")
    print("   FROM system.billing.usage GROUP BY usage_date ORDER BY usage_date DESC")
    
    print("\n4. Job cost breakdown:")
    print("   SELECT job_name, sum(usage_quantity) AS dbus")
    print("   FROM system.billing.usage WHERE job_id IS NOT NULL")
    print("   GROUP BY job_name ORDER BY dbus DESC LIMIT 20")

# COMMAND ----------

# DBTITLE 1,Cell 3: Serverless vs Classic
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 3: Serverless vs Classic Cost Comparison
# MAGIC
# MAGIC Serverless charges per DBU-second with no idle time. Classic clusters charge while running (including idle).

# COMMAND ----------

print("Serverless vs Classic Compute Comparison:")
print("-" * 70)
print(f"{'Aspect':<25} {'Serverless':<22} {'Classic':<23}")
print("-" * 70)
print(f"{'Provisioning':<25} {'Auto (seconds)':<22} {'Manual (minutes)':<23}")
print(f"{'Idle cost':<25} {'Zero (pay per use)':<22} {'Yes (while running)':<23}")
print(f"{'Scaling':<25} {'Auto (instant)':<22} {'Auto (minutes)':<23}")
print(f"{'Max concurrency':<25} {'High (shared pool)':<22} {'Limited per cluster':<23}")
print(f"{'Infrastructure':<25} {'Managed':<22} {'User-managed':<23}")
print(f"{'Best for':<25} {'Notebooks, jobs':<22} {'Custom configs, JARs':<23}")
print(f"{'Cost model':<25} {'Per DBU-second':<22} {'Per DBU-hour':<23}")
print("-" * 70)

print("\nCost Optimization Strategies:")
print("-" * 70)
strategies = [
    ("1. Use serverless", "No idle costs, auto-scaling, pay per use"),
    ("2. Auto-termination", "Set 15-30 min auto-terminate on interactive clusters"),
    ("3. Right-size compute", "Match cluster size to workload - dont over-provision"),
    ("4. Use Photon", "2-5x faster SQL/Spark - fewer DBUs for same work"),
    ("5. Tag everything", "Attribute costs to teams/projects/environments"),
    ("6. Schedule jobs", "Run batch jobs off-peak, use job clusters (auto-terminate)"),
    ("7. Optimize queries", "ZORDER, liquid clustering, partitioning reduce compute"),
    ("8. Use spot instances", "For non-critical workloads (classic clusters)"),
    ("9. Consolidate clusters", "One large cluster > many small ones for same workload"),
    ("10. Monitor daily", "Set up daily cost alerts and weekly reviews"),
]
for strategy, desc in strategies:
    print(f"  {strategy:25s} - {desc}")

# Create a cost tracking table
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.cost.daily_spend AS
    SELECT
        date_add(current_date(), -1) AS spend_date,
        'serverless' AS compute_type,
        125.50 AS estimated_cost_usd,
        850 AS dbus
    UNION ALL
    SELECT
        date_add(current_date(), -1),
        'sql_warehouse',
        45.20,
        320
    UNION ALL
    SELECT
        date_add(current_date(), -1),
        'jobs_cluster',
        78.90,
        615
""")
print("\nSample daily spend tracking table:")
spark.table("demo.cost.daily_spend").display()

# COMMAND ----------

# DBTITLE 1,Cell 4: Budgeting & Tagging
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 4: Budgeting and Tagging
# MAGIC
# MAGIC Set up budgets and tagging to control costs and attribute spending.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

print("Budget Management:")
print("-" * 60)
print("  - Create budgets in account console")
print("  - Set alerts when spending exceeds threshold")
print("  - Filter by workspace, team, or custom tags")
print("  - Track against monthly or quarterly budgets")

print("\nTagging Best Practices:")
print("-" * 60)
print("  1. Set up tags from day one (cant retroactively tag)")
print("  2. Use standard tag keys: team, project, environment, cost_center")
print("  3. Tag workspaces, clusters, SQL warehouses, jobs")
print("  4. Tags propagate to system.billing.usage")
print("  5. Use compute policies to enforce tagging")

print("\nExample compute policy (JSON):")
policy = {
    "name": "serverless-only-policy",
    "description": "Restrict to serverless compute with required tags",
    "definition": [
        {"Key": "spark_version", "Type": "fixed", "Value": "auto"},
        {"Key": "cluster_type", "Type": "allowlist", "Values": ["serverless"]},
        {"Key": "custom_tags.team", "Type": "required", "Value": ""},
        {"Key": "custom_tags.project", "Type": "required", "Value": ""},
    ]
}
import json
print(json.dumps(policy, indent=2))

print("\nCompute Policy API:")
print("  w.cluster_policies.create(name=..., policy=...)")
print("  w.cluster_policies.list()")
print("  w.cluster_policies.edit(policy_id=..., policy=...)")

# COMMAND ----------

# DBTITLE 1,Key Takeaways
# Databricks notebook source
# MAGIC %md
# MAGIC # Key Takeaways
# MAGIC
# MAGIC | Feature | Benefit |
# MAGIC |---------|----------|
# MAGIC | **system.billing** | Full visibility into DBU consumption |
# MAGIC | **Compute policies** | Control who creates what compute |
# MAGIC | **Budgeting** | Alerts when spending exceeds thresholds |
# MAGIC | **Tagging** | Attribute costs to teams and projects |
# MAGIC | **Serverless** | No idle costs, pay per use, auto-scaling |
# MAGIC | **Photon** | 2-5x faster execution = fewer DBUs |
# MAGIC | **Right-sizing** | Match compute to workload |
# MAGIC
# MAGIC ## FinOps Workflow
# MAGIC 1. **Observe**: Query system.billing for spending patterns
# MAGIC 2. **Attribute**: Use tags to assign costs to teams/projects
# MAGIC 3. **Control**: Set budgets, compute policies, auto-termination
# MAGIC 4. **Optimize**: Serverless, Photon, right-sizing, query optimization
# MAGIC 5. **Review**: Weekly cost reviews with team dashboards

# COMMAND ----------

