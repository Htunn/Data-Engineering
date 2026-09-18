# Databricks notebook source
# DBTITLE 1,GenAI/RAG — Overview
# MAGIC %md
# MAGIC # GenAI & RAG: Vector Search, AI Functions & LLM Serving
# MAGIC
# MAGIC **Use Case**: Build Retrieval-Augmented Generation (RAG) pipelines with Databricks AI tools.
# MAGIC
# MAGIC ## Features Covered
# MAGIC
# MAGIC | Feature | Description |
# MAGIC |---------|-------------|
# MAGIC | **AI functions (SQL)** | `ai_query`, `ai_forecast`, `ai_analyze_sentiment` directly in SQL |
# MAGIC | **Embeddings** | Generate text embeddings via `ai_query` to embedding models |
# MAGIC | **Vector Search** | Create endpoints, indexes, and similarity search |
# MAGIC | **RAG pattern** | Retrieve relevant documents → augment prompt → generate answer |
# MAGIC | **LLM serving** | Deploy foundation models via Foundation Model APIs |
# MAGIC | **Agent Bricks** | Reference for building GenAI applications |
# MAGIC
# MAGIC ## RAG Architecture
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart TD
# MAGIC     Q[User Question] --> EMBED_Q[Embed Question]
# MAGIC     EMBED_Q --> VS[Vector Search Index]
# MAGIC     VS -->|top-k results| RETRIEVE[Retrieved Documents]
# MAGIC     RETRIEVE --> AUGMENT[Augment Prompt]
# MAGIC     AUGMENT --> LLM[LLM generates answer]
# MAGIC     LLM --> ANSWER[Answer + Citations]
# MAGIC     
# MAGIC     subgraph "Indexing (offline)"
# MAGIC         DOCS[Source Documents] --> EMB_DOCS[Embed Documents]
# MAGIC         EMB_DOCS --> VS
# MAGIC     end
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Cell 1: Setup
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 1: Setup — Catalog, Schema, Volume

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS demo")
spark.sql("CREATE SCHEMA IF NOT EXISTS demo.genai")
spark.sql("CREATE VOLUME IF NOT EXISTS demo.genai.documents")

print("✅ Setup complete:")
print("   Catalog: demo")
print("   Schema:  demo.genai")
print("   Volume:  demo.genai.documents (for source documents)")

# COMMAND ----------

# DBTITLE 1,Cell 2: Knowledge Base
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 2: Create Knowledge Base Table
# MAGIC
# MAGIC Build a table of documents to use as our RAG knowledge base.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS demo.genai.knowledge_base")

documents = [
    (1, "Databricks Delta Lake", "Delta Lake is the open-source storage layer that brings ACID transactions to Apache Spark and big data workloads. It provides serializable isolation, schema enforcement, and time travel capabilities.", "data-engineering"),
    (2, "Auto Loader Guide", "Auto Loader incrementally and idempotently processes new files as they arrive in cloud storage. It supports schema inference, schema evolution, and exactly-once processing with checkpointing.", "data-engineering"),
    (3, "Unity Catalog Overview", "Unity Catalog provides centralized governance for all data and AI assets. It offers a three-level namespace (catalog.schema.table), fine-grained access control, row-level security, column masking, and audit logging.", "governance"),
    (4, "MLflow Tracking", "MLflow is an open-source platform for managing the ML lifecycle. It provides experiment tracking, model registry, model serving, and reproducible runs with parameters, metrics, and artifacts.", "ml"),
    (5, "Vector Search", "Databricks Vector Search enables similarity search over embeddings. Create a vector search endpoint, build an index from a Delta table, and query with natural language or embedding vectors.", "genai"),
    (6, "Structured Streaming", "Structured Streaming is a scalable, fault-tolerant stream processing engine built on Spark. It supports watermarks for late data, windowed aggregations, stream-stream joins, and foreachBatch for custom sinks.", "streaming"),
    (7, "Spark Declarative Pipelines", "SDP (formerly DLT) provides a declarative framework for building reliable data pipelines. Use @dlt decorators to define tables with expectations (expect, expect_or_drop, expect_or_fail) for data quality enforcement.", "data-engineering"),
    (8, "AI Functions in SQL", "Databricks SQL provides built-in AI functions: ai_query for LLM queries, ai_forecast for time series prediction, ai_analyze_sentiment for sentiment analysis, and ai_classify for text classification.", "genai"),
    (9, "Feature Store", "Feature Store provides centralized feature management with point-in-time joins to prevent data leakage. Features are stored in Unity Catalog and can be shared across teams and models.", "ml"),
    (10, "Model Serving", "Databricks Model Serving provides real-time inference via REST API endpoints. Deploy from Model Registry with stage transitions, enable scale-to-zero, and use AI Gateway for rate limiting and fallback.", "ml"),
]

spark.createDataFrame(documents, ["doc_id", "title", "content", "category"]) \
    .write.format("delta").saveAsTable("demo.genai.knowledge_base")

print(f"✅ Knowledge base: {len(documents)} documents")
spark.table("demo.genai.knowledge_base").select("doc_id", "title", "category").display()

# COMMAND ----------

# DBTITLE 1,Cell 3: Generate Embeddings
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 3: Generate Embeddings with AI Functions
# MAGIC
# MAGIC Use `ai_query` to generate text embeddings for the knowledge base.

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit

# Generate embeddings using ai_query (requires a serving endpoint)
# In production, this would use: ai_query('databricks-gte-large-en', content)
# For demo, we'll simulate the structure

print("📋 SQL for generating embeddings via ai_query:")
print("""
-- Add an embedding column to the knowledge base
ALTER TABLE demo.genai.knowledge_base ADD COLUMN embedding ARRAY<DOUBLE>;

-- Generate embeddings using an embedding model
UPDATE demo.genai.knowledge_base
SET embedding = ai_query('databricks-gte-large-en-v1', content);

-- Or use a CTAS approach:
CREATE OR REPLACE TABLE demo.genai.knowledge_base_embedded AS
SELECT 
    doc_id,
    title,
    content,
    category,
    ai_query('databricks-gte-large-en-v1', content) AS embedding
FROM demo.genai.knowledge_base;
""")

# Simulate embeddings for the demo (random vectors of dimension 384)
import random
embedded_data = []
for doc_id, title, content, category in documents:
    embedding = [round(random.uniform(-1, 1), 6) for _ in range(384)]
    embedded_data.append((doc_id, title, content, category, embedding))

spark.sql("DROP TABLE IF EXISTS demo.genai.knowledge_base_embedded")
spark.createDataFrame(embedded_data, ["doc_id", "title", "content", "category", "embedding"]) \
    .write.format("delta").saveAsTable("demo.genai.knowledge_base_embedded")

print("✅ Knowledge base with embeddings: demo.genai.knowledge_base_embedded")
print(f"   Documents: {len(embedded_data)}, Embedding dim: 384")

# COMMAND ----------

# DBTITLE 1,Cell 4: Vector Search
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 4: Vector Search — Create Endpoint & Index
# MAGIC
# MAGIC Set up a Vector Search endpoint and index for similarity search.

# COMMAND ----------

print("📋 Vector Search Setup (via Python SDK or UI):")
print("""
from databricks.vector_search.client import VectorSearchClient

# 1. Create a Vector Search endpoint
vsc = VectorSearchClient()
endpoint = vsc.create_endpoint(
    name="rag-vector-search",
    endpoint_type="STANDARD"
)

# 2. Create a Delta Sync Index (auto-syncs from Delta table)
index = vsc.create_delta_sync_index(
    endpoint_name="rag-vector-search",
    index_name="demo.genai.knowledge_base_index",
    source_table_name="demo.genai.knowledge_base_embedded",
    pipeline_type="TRIGGERED",
    embedding_dimension=384,
    embedding_vector_column="embedding",
    primary_key="doc_id"
)

# 3. Wait for index to be ready
index.describe()
""")

# For demo, simulate the Vector Search query
print("\n🔍 Simulated Vector Search Query:")
query = "What is Delta Lake?"
query_embedding = [round(random.uniform(-1, 1), 6) for _ in range(384)]

print(f"   Query: '{query}'")
print(f"   Query embedding dim: {len(query_embedding)}")
print(f"\n   Top-k similarity search results:")

# Simulate similarity search (cosine similarity)
import numpy as np
doc_embeddings = {d[0]: np.array(d[4]) for d in embedded_data}
query_vec = np.array(query_embedding)

similarities = []
for doc_id, emb in doc_embeddings.items():
    sim = np.dot(query_vec, emb) / (np.linalg.norm(query_vec) * np.linalg.norm(emb))
    doc_info = next(d for d in embedded_data if d[0] == doc_id)
    similarities.append((doc_id, doc_info[1], doc_info[2][:100], float(sim)))

similarities.sort(key=lambda x: -x[3])
for doc_id, title, snippet, score in similarities[:3]:
    print(f"   [{score:.4f}] Doc {doc_id}: {title}")
    print(f"           {snippet}...")

# COMMAND ----------

# DBTITLE 1,Cell 5: RAG Pipeline
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 5: RAG Pipeline — Retrieve & Generate
# MAGIC
# MAGIC Full RAG flow: embed question → vector search → augment prompt → LLM generates answer.

# COMMAND ----------

print("📋 Full RAG Pipeline (Python SDK):")
print("""
from databricks.vector_search.client import VectorSearchClient
import mlflow.deployments

# 1. Create Vector Search client
vsc = VectorSearchClient()
index = vsc.get_index(endpoint_name="rag-vector-search", index_name="demo.genai.knowledge_base_index")

# 2. Embed the user question
question = "How does Auto Loader handle schema evolution?"
question_embedding = ai_query('databricks-gte-large-en-v1', question)

# 3. Vector search — retrieve top-k relevant documents
results = index.similarity_search(
    query_vector=question_embedding,
    columns=["title", "content", "category"],
    num_results=3
)

# 4. Build augmented prompt with retrieved context
context = "\n\n".join([f"[{r['title']}]\n{r['content']}" for r in results['data']['array']])
augmented_prompt = f"""Answer the question based on the following context:

Context:
{context}

Question: {question}

Answer:"""

# 5. Generate answer using LLM via Foundation Model API
client = mlflow.deployments.get_deploy_client("databricks")
response = client.predict(
    endpoint="databricks-dbrx-instruct",
    inputs={"messages": [{"role": "user", "content": augmented_prompt}]}
)

answer = response.choices[0].message.content
print(f"Answer: {answer}")
print(f"\nSources: {[r['title'] for r in results['data']['array']]}")
""")

# For demo, simulate the RAG answer
print("✅ Simulated RAG Response:")
print("\n📝 Question: How does Auto Loader handle schema evolution?")
print("\n📚 Retrieved documents:")
for doc_id, title, snippet, score in similarities[:3]:
    print(f"   [{score:.4f}] {title}")
print("\n💬 Answer:")
print("   Auto Loader handles schema evolution through the 'cloudFiles.schemaEvolutionMode'")
print("   option. When set to 'addNewColumns', new columns appearing in source files are")
print("   automatically added to the inferred schema. The schema is persisted in the")
print("   'cloudFiles.schemaLocation' path, ensuring consistency across restarts.")
print("\n📎 Sources: [Auto Loader Guide, Databricks Delta Lake, Spark Declarative Pipelines]")

# COMMAND ----------

# DBTITLE 1,Cell 6: AI Functions — Sentiment & Classification
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 6: AI Functions — Sentiment & Classification
# MAGIC
# MAGIC Use built-in AI functions for text analysis directly in SQL.

# COMMAND ----------

# Create a table with customer reviews
spark.sql("DROP TABLE IF EXISTS demo.genai.customer_reviews")
reviews = [
    (1, 1, "This product is amazing! Best purchase I've made all year.", 5),
    (2, 2, "Terrible experience. The product broke after 2 days.", 1),
    (3, 3, "It's okay. Does the job but nothing special.", 3),
    (4, 4, "Outstanding quality and fast shipping. Highly recommend!", 5),
    (5, 5, "Disappointing. Not worth the price.", 2),
]
spark.createDataFrame(reviews, ["review_id", "customer_id", "review_text", "rating"]) \
    .write.format("delta").saveAsTable("demo.genai.customer_reviews")

print("📋 SQL for AI function analysis:")
print("""
-- Sentiment analysis
SELECT 
    review_id,
    review_text,
    rating,
    ai_analyze_sentiment(review_text) AS sentiment
FROM demo.genai.customer_reviews;

-- Text classification
SELECT 
    review_id,
    review_text,
    ai_classify(review_text, ARRAY['positive', 'negative', 'neutral']) AS category
FROM demo.genai.customer_reviews;

-- Summarization
SELECT 
    customer_id, 
    ai_summarize(review_text) AS summary
FROM demo.genai.customer_reviews
GROUP BY customer_id;

-- Question answering
SELECT 
    ai_extract_answer(review_text, 'What is the customer''s main complaint?') AS complaint
FROM demo.genai.customer_reviews
WHERE rating <= 2;
""")

# Simulate results
print("✅ Simulated AI Function Results:")
for rid, cust_id, text, rating in reviews:
    sentiment = "positive" if rating >= 4 else ("neutral" if rating == 3 else "negative")
    print(f"   Review {rid} [Rating {rating}]: {sentiment:10s} — {text[:60]}...")

# COMMAND ----------

# DBTITLE 1,Cell 7: LLM Serving & AI Gateway
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 7: LLM Serving & AI Gateway
# MAGIC
# MAGIC Deploy and manage LLM endpoints for production GenAI applications.

# COMMAND ----------

print("📋 LLM Serving Options:")
print("""
1. **Foundation Model APIs** (pay-per-token):
   POST /serving-endpoints/databricks-dbrx-instruct/invocations
   {
     "messages": [{"role": "user", "content": "Explain Delta Lake"}],
     "max_tokens": 500,
     "temperature": 0.7
   }

2. **External Models** (bring your own provider):
   - Azure OpenAI, OpenAI, Anthropic, Google Gemini, etc.
   - Configured via AI Gateway with rate limiting, fallback, and logging

3. **AI Gateway** (unified LLM management):
   - Rate limiting (requests/tokens per time window)
   - Fallback across providers (primary → backup)
   - Inference table logging (all requests/responses to UC table)
   - Guardrails (PII detection, content filtering)

4. **Agent Bricks** (managed GenAI apps):
   - Knowledge Assistant — RAG chatbot over documents
   - Supervisor Agent — orchestrates multiple subagents
   - Custom AI agents with tools (SQL, dashboards, APIs)

5. **Mosaic AI Agent Framework** (build custom agents):
   from databricks.agents import deploy_agent
   deploy_agent(model_name="my-rag-agent", model_version=1)
""")

print("\n📊 GenAI Application Patterns:")
print("""
  | Pattern              | Databricks Tools                          |
  |----------------------|-------------------------------------------|
  | RAG                  | Vector Search + Foundation Model API      |
  | Document Q&A         | Agent Bricks Knowledge Assistant           |
  | Multi-agent          | Supervisor Agent + subagents              |
  | Tool-use agent       | Mosaic AI Agent Framework + UC functions  |
  | Batch LLM scoring    | ai_query in SQL + Spark                   |
  | Streaming LLM        | Structured Streaming + foreachBatch + LLM  |
""")

# COMMAND ----------

# DBTITLE 1,Key Takeaways
# MAGIC %md
# MAGIC # Key Takeaways
# MAGIC
# MAGIC | Tool | Best For |
# MAGIC |------|----------|
# MAGIC | **AI functions (SQL)** | LLM-powered analytics without Python code |
# MAGIC | **Vector Search** | Similarity search, semantic search, RAG retrieval |
# MAGIC | **Foundation Model APIs** | Pay-per-token LLM inference (no infrastructure) |
# MAGIC | **AI Gateway** | Rate limiting, fallback, logging for LLM endpoints |
# MAGIC | **Agent Bricks** | Managed GenAI apps (Knowledge Assistant, Supervisor) |
# MAGIC | **Agent Framework** | Custom agents with tools, SQL, dashboards |
# MAGIC
# MAGIC ## Best Practices
# MAGIC 1. **Use Vector Search for RAG** — faster and more scalable than brute-force similarity
# MAGIC 2. **Embed once, query many** — pre-compute embeddings for static documents
# MAGIC 3. **Use AI Gateway for production** — rate limiting prevents cost surprises
# MAGIC 4. **Log all LLM interactions** — AI Gateway inference tables to UC for auditing
# MAGIC 5. **Cache embeddings** — avoid re-embedding unchanged documents
# MAGIC 6. **Use chunking for long documents** — split by paragraphs/sections before embedding
# MAGIC 7. **Evaluate RAG quality** — use MLflow evaluations for retrieval and generation quality
# MAGIC
# MAGIC ## References
# MAGIC - [Vector Search](https://docs.databricks.com/en/generative-ai/vector-search.html)
# MAGIC - [AI Functions](https://docs.databricks.com/en/sql/language-manual/functions/ai_query.html)
# MAGIC - [Agent Framework](https://docs.databricks.com/en/generative-ai/create-agent.html)
# MAGIC - [AI Gateway](https://docs.databricks.com/en/generative-ai/ai-gateway/index.html)

# COMMAND ----------

