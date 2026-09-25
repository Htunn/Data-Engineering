# Databricks notebook source
# DBTITLE 1,Overview
# Databricks notebook source
# MAGIC %md
# MAGIC # Agent Development — Custom Agents, MCP & Supervisor Agent
# MAGIC
# MAGIC **Use Case**: Build, deploy, and evaluate AI agents on Databricks.
# MAGIC
# MAGIC ## Agent Types on Databricks
# MAGIC
# MAGIC | Type | Description | Best For |
# MAGIC |------|-------------|----------|
# MAGIC | **AI Playground** | No-code UI to prototype agents with LLMs + tools | Quick prototyping |
# MAGIC | **Knowledge Assistant** | Managed RAG chatbot over your documents | Document Q&A |
# MAGIC | **Supervisor Agent** | Orchestrates multiple subagents + tools | Complex multi-domain questions |
# MAGIC | **Custom Agent** | Python code with LangGraph/LlamaIndex/OpenAI | Full control, custom logic |
# MAGIC | **Genie Agent** | Natural-language Q&A over structured data | Data analytics |
# MAGIC
# MAGIC ## Key Concepts
# MAGIC
# MAGIC - **Tool calling**: Agents call tools (UC functions, MCP servers, APIs) to answer questions
# MAGIC - **MCP (Model Context Protocol)**: Standard protocol for connecting agents to data/tools
# MAGIC - **MLflow Tracing**: Instrument agents to collect telemetry for evaluation
# MAGIC - **Agent evaluation**: LLM judges score agent quality (relevance, groundedness, safety)
# MAGIC - **Deployment**: Agents deploy as Databricks Apps (serverless, OAuth)

# COMMAND ----------

# DBTITLE 1,Cell 1: Setup
# Databricks notebook source
spark.sql("CREATE SCHEMA IF NOT EXISTS demo.agents")
print("✅ Schema demo.agents created")

# COMMAND ----------

# DBTITLE 1,Cell 2: UC Functions as Tools
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 2: Unity Catalog Functions as Agent Tools
# MAGIC
# MAGIC UC functions can serve as agent tools — the agent calls them to query data or perform actions.

# COMMAND ----------

# Create a UC function that an agent can call as a tool
spark.sql("""
    CREATE OR REPLACE FUNCTION demo.agents.get_customer_tier(customer_id INT)
    RETURNS STRING
    COMMENT 'Returns the loyalty tier for a given customer ID'
    LANGUAGE SQL
    RETURN CASE
        WHEN customer_id < 50 THEN 'Bronze'
        WHEN customer_id < 100 THEN 'Silver'
        WHEN customer_id < 150 THEN 'Gold'
        ELSE 'Platinum'
    END
""")
print("✅ Created UC function: demo.agents.get_customer_tier")

# Test the function
result = spark.sql("SELECT demo.agents.get_customer_tier(25) AS tier")
print(f"   get_customer_tier(25) = {result.collect()[0]['tier']}")
result = spark.sql("SELECT demo.agents.get_customer_tier(125) AS tier")
print(f"   get_customer_tier(125) = {result.collect()[0]['tier']}")

# Create a SQL-based tool for revenue lookup
spark.sql("""
    CREATE OR REPLACE FUNCTION demo.agents.get_revenue_summary(month_str STRING)
    RETURNS TABLE (total_revenue DOUBLE, order_count INT)
    COMMENT 'Returns total revenue and order count for a given month (format: YYYY-MM)'
    LANGUAGE SQL
    RETURN SELECT
        CAST(rand() * 100000 AS DOUBLE) AS total_revenue,
        CAST(rand() * 500 AS INT) AS order_count
""")
print("\n✅ Created UC function: demo.agents.get_revenue_summary (table-returning)")
spark.sql("SELECT * FROM demo.agents.get_revenue_summary('2025-01')").display()

# COMMAND ----------

# DBTITLE 1,Cell 3: AI Functions
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 3: AI Functions — Call LLMs from SQL
# MAGIC
# MAGIC Use `ai_query()` to call LLMs directly from SQL — useful for agent-like tasks without deploying a full agent.

# COMMAND ----------

# Create sample customer data
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.agents.customer_feedback AS
    SELECT
        id AS feedback_id,
        CASE
            WHEN id % 3 = 0 THEN 'The product is amazing! Best purchase I have made this year.'
            WHEN id % 3 = 1 THEN 'Terrible experience. The app keeps crashing and support was unhelpful.'
            ELSE 'It is okay. Some features are good but the UI needs improvement.'
        END AS feedback_text,
        concat('2025-0', cast((id % 9) + 1 as string), '-15') AS feedback_date
    FROM range(100)
""")
print("✅ Created customer feedback table")

# Use ai_analyze_sentiment to classify feedback
print("\n🔍 Sentiment analysis with ai_analyze_sentiment():")
spark.sql("""
    SELECT
        feedback_id,
        feedback_text,
        ai_analyze_sentiment(feedback_text) AS sentiment
    FROM demo.agents.customer_feedback
    LIMIT 5
""").display()

# Use ai_query for custom LLM tasks
print("\n🤖 Custom LLM query with ai_query():")
try:
    spark.sql("""
        SELECT ai_query(
            'databricks-meta-llama-3-1-70b-instruct',
            'Summarize this customer feedback in one sentence: ' ||
            'The product is amazing but shipping was slow and the box was damaged.'
        ) AS summary
    """).display()
except Exception as e:
    print(f"   ℹ️ ai_query requires a serving endpoint: {str(e)[:100]}")
    print("   Create a serving endpoint first, then use ai_query() to call it from SQL.")

# COMMAND ----------

# DBTITLE 1,Cell 4: Supervisor Agent
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 4: Supervisor Agent — Multi-Agent Orchestration
# MAGIC
# MAGIC Supervisor Agent coordinates multiple subagents (Genie, custom agents, UC functions, MCP servers) to answer complex questions.
# MAGIC
# MAGIC **Architecture:**
# MAGIC ```
# MAGIC User Question → Supervisor Agent → Routes to:
# MAGIC   ├── Genie Agent (structured data Q&A)
# MAGIC   ├── Knowledge Assistant (document Q&A)
# MAGIC   ├── UC Function (get_customer_tier)
# MAGIC   └── MCP Server (external API)
# MAGIC ```

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Create a Supervisor Agent via SDK
print("🧙 Creating Supervisor Agent configuration...")

supervisor_config = {
    "display_name": "Customer Analytics Supervisor",
    "description": "Orchestrates customer data queries, feedback analysis, and tier lookups",
    "instructions": "You are a customer analytics assistant. Route questions about customer tiers to the get_customer_tier function. Route sentiment questions to the feedback analysis agent. Route general data questions to the Genie agent."
}

print("📋 Supervisor Agent configuration:")
import json
print(json.dumps(supervisor_config, indent=2))

# In production, create the supervisor agent:
# from databricks.sdk.service.supervisoragents import SupervisorAgent
# created = w.supervisor_agents.create_supervisor_agent(
#     supervisor_agent=SupervisorAgent(
#         display_name=supervisor_config["display_name"],
#         description=supervisor_config["description"],
#         instructions=supervisor_config["instructions"],
#     )
# )
# print(f"✅ Created supervisor agent: {created}")

print("\n💡 To create in production:")
print("   1. Go to Agents in the Databricks UI")
print("   2. Click Create Agent → Supervisor Agent")
print("   3. Add subagents: Genie Agent, UC functions, MCP servers")
print("   4. Provide descriptions for each tool (supervisor uses these for routing)")
print("   5. Add instructions for the supervisor")
print("   6. Test and deploy")

print("\n🔧 Supported subagent/tool types:")
print("   - Genie Agents (structured data Q&A)")
print("   - Agent endpoints (custom agents)")
print("   - Unity Catalog functions (SQL/Python tools)")
print("   - MCP servers (external APIs, code execution)")
print("   - AI Search indexes (document retrieval)")

# COMMAND ----------

# DBTITLE 1,Cell 5: Custom Agent Code
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 5: Custom Agent Code Pattern
# MAGIC
# MAGIC A custom agent is a Python application deployed as a Databricks App. Here's the code pattern.

# COMMAND ----------

agent_code = '''
# agent.py — Custom agent deployed as a Databricks App
# Requires: pip install databricks-sdk mlflow openai

from databricks.sdk import WorkspaceClient
import mlflow
import json

# 1. Initialize the agent
w = WorkspaceClient()

# 2. Define tools the agent can call
def get_customer_tier(customer_id: int) -> str:
    """Look up customer loyalty tier."""
    result = w.statement_execution.execute_sql(
        statement=f"SELECT demo.agents.get_customer_tier({customer_id}) AS tier"
    )
    return result.fetch().fetch_one().as_dict()["tier"]

def analyze_sentiment(text: str) -> str:
    """Analyze sentiment of customer feedback."""
    result = w.statement_execution.execute_sql(
        statement=f"SELECT ai_analyze_sentiment('{text}') AS sentiment"
    )
    return result.fetch().fetch_one().as_dict()["sentiment"]

# 3. Agent loop (simplified — use LangGraph in production)
tools = [get_customer_tier, analyze_sentiment]

# 4. Log to MLflow for tracing and evaluation
mlflow.set_experiment("/demo/agents/customer_analytics")

with mlflow.start_run() as run:
    mlflow.log_param("agent_type", "custom_tool_calling")
    mlflow.log_param("tools", [t.__name__ for t in tools])

    # 5. Deploy as Databricks App
    # The agent runs as a Flask/Streamlit app on Databricks Apps
    # OAuth authentication, serverless compute, auto-scaling
    print(f"Agent ready. MLflow run: {run.info.run_id}")

# app.yaml (Databricks Apps deployment config)
# environment: default
# compute:
#   cpu: 1
#   memory: 2Gi
# entrypoint: python agent.py
# depends_on:
#   - pip install databricks-sdk mlflow openai
'''

print(agent_code)
print("\n💡 Deploy this as a Databricks App:")
print("   1. Save as agent.py in a Git folder")
print("   2. Create app.yaml with compute config")
print("   3. Deploy via DAB or CLI: databricks apps deploy")
print("   4. Agent runs on serverless with OAuth authentication")

# COMMAND ----------

# DBTITLE 1,Key Takeaways
# Databricks notebook source
# MAGIC %md
# MAGIC # Key Takeaways
# MAGIC
# MAGIC | Feature | Benefit |
# MAGIC |---------|----------|
# MAGIC | **UC Functions as Tools** | Agents call SQL/Python functions via Unity Catalog |
# MAGIC | **AI Functions** | `ai_query`, `ai_analyze_sentiment` — call LLMs from SQL |
# MAGIC | **Supervisor Agent** | Orchestrate Genie + custom agents + MCP servers |
# MAGIC | **Custom Agents** | Full Python control with LangGraph/LlamaIndex |
# MAGIC | **MLflow Tracing** | Instrument agents for evaluation and monitoring |
# MAGIC | **Databricks Apps** | Serverless deployment with OAuth authentication |
# MAGIC
# ## Agent Development Workflow
# MAGIC 1. **Prototype** in AI Playground (no code)
# MAGIC 2. **Add tools** (UC functions, MCP servers, Genie agents)
# MAGIC 3. **Build** custom agent in Python (if needed)
# MAGIC 4. **Evaluate** with MLflow Tracing + LLM judges
# MAGIC 5. **Deploy** as Databricks App
# MAGIC 6. **Monitor** production traces with MLflow

# COMMAND ----------

