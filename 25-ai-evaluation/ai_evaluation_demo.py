# Databricks notebook source
# DBTITLE 1,Overview & Setup
# Databricks notebook source
# MAGIC %md
# MAGIC # AI Evaluation and Observability - MLflow GenAI
# MAGIC
# MAGIC **Use Case**: Evaluate, trace, and monitor AI agents and LLM applications.
# MAGIC
# MAGIC ## Features Covered
# MAGIC
# MAGIC | Feature | Description |
# MAGIC |---------|-------------|
# MAGIC | **MLflow Tracing** | Instrument agents to collect telemetry - every LLM call, tool call |
# MAGIC | **LLM Judges** | Built-in AI evaluators: relevance, groundedness, safety |
# MAGIC | **Evaluation Datasets** | Curated test sets to measure agent quality |
# MAGIC | **Production Monitoring** | Monitor live traces with the same judges |
# MAGIC | **Human Feedback** | Review App for vibe checks + labeling sessions |
# MAGIC | **Custom Judges** | Define your own evaluation criteria in natural language |

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS demo.ai_eval")
print("Schema demo.ai_eval created")

# Create a sample evaluation dataset
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.ai_eval.eval_dataset AS
    SELECT
        struct(
            'What is the return policy for electronics?' AS question,
            'Electronics can be returned within 30 days with original receipt.' AS expected_answer
        ) AS request,
        'Electronics have a 30-day return policy with receipt.' AS response,
        'relevance' AS metric
    UNION ALL
    SELECT
        struct(
            'How do I reset my password?' AS question,
            'Click forgot password on the login page and follow the email instructions.' AS expected_answer
        ) AS request,
        'I dont know. Please contact support.' AS response,
        'relevance' AS metric
    UNION ALL
    SELECT
        struct(
            'What are your business hours?' AS question,
            'We are open Monday to Friday, 9 AM to 5 PM EST.' AS expected_answer
        ) AS request,
        'We are open Monday to Friday, 9 AM to 5 PM EST.' AS response,
        'groundedness' AS metric
""")
print("Created evaluation dataset with 3 test cases")
spark.table("demo.ai_eval.eval_dataset").display()

# COMMAND ----------

# DBTITLE 1,Cell 2: MLflow Tracing
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 2: MLflow Tracing - Instrument Your Agent
# MAGIC
# MAGIC MLflow Tracing records every step of your agent's execution - LLM calls, tool calls, and intermediate reasoning.

# COMMAND ----------

import mlflow
from datetime import datetime

mlflow.set_experiment("/demo/ai_eval/tracing_demo")

@mlflow.trace
def retrieve_context(question: str) -> str:
    """Simulate RAG retrieval step."""
    return "Return policy: 30 days for electronics. Password reset via forgot password link."

@mlflow.trace
def generate_response(question: str, context: str) -> str:
    """Simulate LLM generation step."""
    if "return" in question.lower():
        return "Electronics can be returned within 30 days with the original receipt."
    elif "password" in question.lower():
        return "Click forgot password on the login page and follow the email instructions."
    else:
        return "Please contact support for assistance."

@mlflow.trace
def agent_route(question: str) -> str:
    """Main agent function - traces all steps."""
    context = retrieve_context(question)
    response = generate_response(question, context)
    return response

with mlflow.start_run(run_name=f"agent_eval_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
    test_questions = [
        "What is the return policy for electronics?",
        "How do I reset my password?",
        "What are your business hours?"
    ]
    for q in test_questions:
        response = agent_route(q)
        mlflow.log_param(f"q_{hash(q) % 10000}", q[:30])
        print(f"Q: {q}")
        print(f"A: {response}")
        print()

print("Traces logged to MLflow. View in the MLflow UI for full trace tree.")

# COMMAND ----------

# DBTITLE 1,Cell 3: LLM Judges
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 3: LLM Judges - Built-in Evaluation Metrics
# MAGIC
# MAGIC LLM judges are AI-powered evaluators that score agent responses on quality dimensions.

# COMMAND ----------

print("Built-in LLM Judges on Databricks:")
print("-" * 60)
judges = [
    ("relevance", "Is the response relevant to the question?"),
    ("groundedness", "Is the response grounded in the provided context?"),
    ("safety", "Does the response contain harmful or unsafe content?"),
    ("helpfulness", "Is the response helpful to the user?"),
    ("correctness", "Is the response factually correct?"),
    ("completeness", "Does the response fully answer the question?"),
]
for name, desc in judges:
    print(f"  {name:20s} - {desc}")

print("\nEvaluation Workflow:")
print("  1. Create evaluation dataset (questions + expected answers)")
print("  2. Run agent against dataset to get responses")
print("  3. Apply LLM judges to score each response")
print("  4. Review scores in MLflow UI")
print("  5. Iterate on agent prompts/tools to improve scores")

# Show the evaluation dataset
print("\nEvaluation dataset:")
eval_df = spark.table("demo.ai_eval.eval_dataset")
print(f"  Test cases: {eval_df.count()}")
print(f"  Columns: {eval_df.columns}")

# In production, use mlflow.evaluate() to run judges:
# result = mlflow.evaluate(
#     data=eval_df.toPandas(),
#     targets="expected_answer",
#     predictions="response",
#     evaluators="default",
#     extra_metrics=[
#         mlflow.metrics.genai.answer_relevance("question"),
#         mlflow.metrics.genai.groundedness("context"),
#     ]
# )
print("\nIn production: Use mlflow.evaluate() with genai metrics to score responses.")

# COMMAND ----------

# DBTITLE 1,Cell 4: Production Monitoring
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 4: Production Monitoring and Human Feedback
# MAGIC
# MAGIC Once an agent is deployed, you need to monitor quality in production and collect human feedback.

# COMMAND ----------

print("Production Monitoring:")
print("-" * 60)
print("  - MLflow Tracing runs on production traffic")
print("  - LLM judges run asynchronously on incoming traces")
print("  - Detects: hallucinations, PII leakage, safety violations, user frustration")
print("  - Set up alerts when quality scores drop below threshold")

print("\nHuman Feedback:")
print("-" * 60)
print("  Development: Use Review App for quick vibe checks")
print("    - Chat UI to interact with agent")
print("    - Collect expert feedback in labeling sessions")
print("    - Use feedback to create ground-truth datasets")
print("  Production: APIs let users annotate traces with feedback")
print("    - Thumbs up/down on responses")
print("    - Free-text feedback")
print("    - Route low-rated traces for review")

print("\nMLOps for GenAI:")
print("-" * 60)
print("  1. Develop: AI Playground -> Custom agent code")
print("  2. Evaluate: MLflow evaluate() with LLM judges")
print("  3. Deploy: Databricks Apps (serverless, OAuth)")
print("  4. Monitor: MLflow Tracing on production traffic")
print("  5. Improve: Human feedback -> new eval dataset -> retrain")

# COMMAND ----------

# DBTITLE 1,Key Takeaways
# Databricks notebook source
# MAGIC %md
# MAGIC # Key Takeaways
# MAGIC
# MAGIC | Feature | Benefit |
# MAGIC |---------|----------|
# MAGIC | **MLflow Tracing** | Full observability of agent execution - every step traced |
# MAGIC | **LLM Judges** | Automated quality scoring - relevance, groundedness, safety |
# MAGIC | **Evaluation Datasets** | Curated test sets to measure and improve agent quality |
# MAGIC | **Production Monitoring** | Continuous quality monitoring on live traffic |
# MAGIC | **Human Feedback** | Expert labeling + user feedback to calibrate judges |
# MAGIC
# MAGIC ## Comparison: Classic MLflow (Module 07) vs GenAI MLflow (Module 25)
# MAGIC | Aspect | Classic MLflow | GenAI MLflow |
# MAGIC |--------|-------------|-------------|
# MAGIC | Metrics | accuracy, F1, RMSE | relevance, groundedness, safety |
# MAGIC | Tracing | Not needed | Every LLM/tool call traced |
# MAGIC | Evaluation | Test set + metrics | LLM judges + human feedback |
# MAGIC | Artifacts | Model weights | Prompts, traces, agent code |
# MAGIC | Monitoring | Drift detection | Quality on production traces |

# COMMAND ----------

