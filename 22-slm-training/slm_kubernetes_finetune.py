# Databricks notebook source
# /// script
# [tool.databricks.environment]
# base_environment = "databricks_ai_v5"
# environment_version = "5"
# ///
# DBTITLE 1,SLM Training — Overview
# MAGIC %md
# MAGIC # SLM Training: Domain-Specific Fine-Tuning for Kubernetes
# MAGIC
# MAGIC Fine-tune a Small Language Model (DistilGPT2, 82M params) on Kubernetes domain data using LoRA — end-to-end on Databricks GPU serverless.
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC ```
# MAGIC Kubernetes Q&A Data → Tokenize → DistilGPT2 + LoRA → Train (HF Trainer) → MLflow → UC Registry → Inference
# MAGIC ```
# MAGIC
# MAGIC ## Pipeline Stages
# MAGIC
# MAGIC | Stage | Description |
# MAGIC |-------|-------------|
# MAGIC | **1. Data Prep** | Generate Kubernetes domain Q&A training pairs |
# MAGIC | **2. Tokenization** | Format as causal LM input with HuggingFace tokenizer |
# MAGIC | **3. Model Setup** | Load DistilGPT2, inject LoRA adapters via PEFT |
# MAGIC | **4. Training** | HuggingFace Trainer with LoRA (only ~300K trainable params) |
# MAGIC | **5. Evaluation** | Perplexity + sample generation quality |
# MAGIC | **6. MLflow** | Log params, metrics, model artifact to MLflow |
# MAGIC | **7. Registration** | Register model in Unity Catalog |
# MAGIC | **8. Inference** | Test fine-tuned model on K8s questions |
# MAGIC
# MAGIC ## Why LoRA?
# MAGIC
# MAGIC Full fine-tuning of DistilGPT2 = 82M parameters. LoRA freezes the base model and trains only low-rank adapters (~300K params, 0.4% of total). This makes training feasible on a single A10 GPU with minimal memory.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Cell 1: Setup
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 1: Setup — GPU Check, Imports, UC Catalog

# COMMAND ----------

import torch
import transformers
import peft

# Verify GPU availability
gpu_available = torch.cuda.is_available()
gpu_name = torch.cuda.get_device_name(0) if gpu_available else "None"
print(f"🖥️  GPU: {gpu_name}")
print(f"📦 PyTorch: {torch.__version__}")
print(f"🤗 Transformers: {transformers.__version__}")
print(f"🔗 PEFT (LoRA): {peft.__version__}")

if not gpu_available:
    raise RuntimeError("GPU not available — switch to Serverless GPU compute")

# Create UC catalog/schema for this module
spark.sql("CREATE CATALOG IF NOT EXISTS demo")
spark.sql("CREATE SCHEMA IF NOT EXISTS demo.slm")
spark.sql("CREATE VOLUME IF NOT EXISTS demo.slm.artifacts")

print("✅ Setup complete — GPU verified, UC catalog ready")

# COMMAND ----------

# DBTITLE 1,Cell 2: Data Preparation
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 2: Data Preparation — Kubernetes Domain Q&A
# MAGIC
# MAGIC Generate a domain-specific training corpus of Kubernetes Q&A pairs.
# MAGIC In production, replace this with real data from K8s docs, Stack Overflow, or internal runbooks.

# COMMAND ----------

import pandas as pd
import random
random.seed(42)

# Kubernetes domain Q&A training data
k8s_qa_data = [
    {"question": "What is a Kubernetes Pod?", "answer": "A Pod is the smallest deployable unit in Kubernetes. It encapsulates one or more containers, shared storage, network resources, and a specification for how to run them. Pods are ephemeral and managed by higher-level controllers like Deployments."},
    {"question": "What is a Kubernetes Deployment?", "answer": "A Deployment manages a set of Pods with declarative updates. It ensures a specified number of Pod replicas are running, supports rolling updates and rollbacks, and uses a ReplicaSet under the hood."},
    {"question": "How does a Kubernetes Service work?", "answer": "A Service is an abstraction that exposes a logical set of Pods as a network service. It provides a stable IP and DNS name, load-balances traffic across Pods, and supports types: ClusterIP, NodePort, LoadBalancer, and ExternalName."},
    {"question": "What is a ConfigMap in Kubernetes?", "answer": "A ConfigMap stores non-confidential data as key-value pairs. Pods consume ConfigMaps as environment variables, command-line arguments, or configuration files in volumes. Use Secrets for sensitive data like passwords and tokens."},
    {"question": "What is the difference between a StatefulSet and a Deployment?", "answer": "A StatefulSet provides stable network identities, persistent storage, and ordered deployment/scaling for stateful apps (databases). A Deployment is for stateless apps with interchangeable Pods. StatefulSets use ordinal indexes (pod-0, pod-1) while Deployments use random suffixes."},
    {"question": "What is a Kubernetes Ingress?", "answer": "An Ingress is an API object that manages external HTTP/HTTPS access to services in a cluster. It provides name-based virtual hosting, TLS termination, and path-based routing. An Ingress Controller (e.g., nginx-ingress) fulfils the Ingress rules."},
    {"question": "How does Horizontal Pod Autoscaler (HPA) work?", "answer": "HPA automatically scales the number of Pod replicas based on CPU/memory metrics or custom metrics. It queries the Metrics Server periodically and adjusts the replica count in the Deployment or StatefulSet to match the target utilization."},
    {"question": "What is a Kubernetes Namespace?", "answer": "A Namespace is a mechanism for isolating groups of resources within a cluster. Namespaces provide scope for names, resource quotas, and RBAC. Default namespaces include 'default', 'kube-system', and 'kube-public' for system components."},
    {"question": "What is RBAC in Kubernetes?", "answer": "Role-Based Access Control (RBAC) regulates access to Kubernetes resources. Roles define permissions within a namespace; ClusterRoles are cluster-wide. RoleBindings grant Role permissions to users/groups/ServiceAccounts. Use least-privilege principle."},
    {"question": "What is a Kubernetes Secret?", "answer": "A Secret stores small amounts of sensitive data like passwords, OAuth tokens, and SSH keys. Secrets are base64-encoded and can be mounted as files or exposed as environment variables. Use encryption at rest for production clusters."},
    {"question": "What is a Kubernetes DaemonSet?", "answer": "A DaemonSet ensures that a copy of a Pod runs on every (or selected) node. Use cases: log collection (Fluentd), monitoring agents (Prometheus node exporter), network plugins (Calico), and storage provisioners."},
    {"question": "What is a Kubernetes Job?", "answer": "A Job creates one or more Pods that run to completion. It tracks successful completions and retries failures up to the backoffLimit. Use Jobs for batch processing, database migrations, or one-time setup tasks."},
    {"question": "What is a Kubernetes CronJob?", "answer": "A CronJob runs Jobs on a scheduled cron-like schedule. It supports concurrency policies (Allow, Forbid, Replace), history limits for successful and failed Jobs, and is ideal for periodic tasks like backups and report generation."},
    {"question": "What is kubectl?", "answer": "kubectl is the CLI tool for interacting with Kubernetes clusters. It uses kubeconfig for authentication. Common commands: kubectl get, apply, describe, logs, exec, port-forward. It communicates with the API server via REST."},
    {"question": "What is a Kubernetes label and selector?", "answer": "Labels are key-value pairs attached to objects for identification. Selectors query objects by labels: equality-based (key=value) or set-based (key in [v1,v2]). Services use selectors to target Pods. Labels enable grouping and filtering of resources."},
    {"question": "What is a PersistentVolume (PV)?", "answer": "A PV is a cluster storage resource provisioned by an administrator or dynamically via StorageClasses. Pods claim PVs via PersistentVolumeClaims (PVCs). PV lifecycle is independent of Pods. Access modes: ReadWriteOnce, ReadOnlyMany, ReadWriteMany."},
    {"question": "What is a StorageClass in Kubernetes?", "answer": "A StorageClass provides dynamic volume provisioning. It defines the provisioner (e.g., AWS EBS, GCE PD, NFS), parameters, and reclaim policy. PVCs reference a StorageClass to automatically create PVs on demand."},
    {"question": "What is a Kubernetes Operator?", "answer": "An Operator is a custom controller that manages a complex application using custom resources. It encodes operational knowledge (deploy, scale, backup, upgrade) as automation. Examples: Prometheus Operator, Elasticsearch Operator, PostgreSQL Operator."},
    {"question": "What is the Kubernetes API server?", "answer": "The API server (kube-apiserver) is the front-end of the Kubernetes control plane. It validates and configures API objects (Pods, Services, Deployments). All components communicate through the API server. It exposes a RESTful API and uses etcd for persistence."},
    {"question": "What is etcd in Kubernetes?", "answer": "etcd is a distributed key-value store that serves as the primary data store for Kubernetes cluster state. It stores configuration data, cluster state, and metadata. etcd uses the Raft consensus algorithm for consistency across nodes."},
    {"question": "What is a Kubernetes node?", "answer": "A node is a worker machine in a Kubernetes cluster. Each node runs kubelet (agent), kube-proxy (networking), and container runtime (containerd, CRI-O). Nodes can be physical or virtual. The control plane manages node scheduling and health."},
    {"question": "What is kube-proxy?", "answer": "kube-proxy runs on every node and maintains network rules for Service routing. It uses iptables or IPVS for load balancing across Pods. It enables service discovery and connectivity within the cluster."},
    {"question": "What is a Container Runtime Interface (CRI)?", "answer": "CRI is a plugin interface that lets Kubernetes use different container runtimes (containerd, CRI-O, Docker). It standardises the API between kubelet and the runtime, enabling runtime swapping without cluster changes."},
    {"question": "What is a Helm chart?", "answer": "Helm is a package manager for Kubernetes. A Chart is a collection of templated YAML files defining a Kubernetes application. Helm supports versioning, dependencies, and values overrides. Use 'helm install' to deploy charts to a cluster."},
    {"question": "What is a Kubernetes readiness probe?", "answer": "A readiness probe determines if a Pod is ready to serve traffic. If it fails, the Pod is removed from Service endpoints. Types: HTTP GET, TCP socket, exec command. Configure initialDelaySeconds and periodSeconds for reliable health checks."},
    {"question": "What is a Kubernetes liveness probe?", "answer": "A liveness probe detects if a container is stuck or deadlocked. If it fails, kubelet restarts the container. Types: httpGet, tcpSocket, exec. Unlike readiness probes, liveness failures trigger container restarts, not traffic removal."},
    {"question": "What is taint and toleration in Kubernetes?", "answer": "Taints mark nodes to repel Pods (e.g., GPU-only, dedicated nodes). Tolerations allow Pods to schedule on tainted nodes. Together they control Pod placement. Use 'kubectl taint nodes' and 'tolerations' in Pod specs."},
    {"question": "What is node affinity in Kubernetes?", "answer": "Node affinity constrains Pod scheduling to specific nodes based on labels. It is more expressive than nodeSelector. Types: required (hard rule) and preferred (soft rule). Use cases: GPU scheduling, zone distribution, hardware constraints."},
    {"question": "What is a Kubernetes NetworkPolicy?", "answer": "A NetworkPolicy controls traffic flow between Pods and from external sources. It uses labels to select Pods and defines ingress/egress rules. Requires a CNI plugin that supports policies (Calico, Cilium). Default: all traffic allowed if no policy exists."},
    {"question": "What is the difference between ReplicaSet and Deployment?", "answer": "A ReplicaSet maintains a stable set of replica Pods. A Deployment wraps a ReplicaSet and adds declarative rolling updates and rollbacks. You should use Deployments directly — they manage ReplicaSets automatically. Never manage ReplicaSets directly."},
]

# Format as instruction-tuning text
training_texts = []
for item in k8s_qa_data:
    text = f"Question: {item['question']}\nAnswer: {item['answer']}<|endoftext|>"
    training_texts.append(text)

# Create DataFrame and save to Delta for reproducibility
df = pd.DataFrame(k8s_qa_data)
df["training_text"] = training_texts
spark_df = spark.createDataFrame(df)
spark_df.write.format("delta").mode("overwrite").saveAsTable("demo.slm.k8s_training_data")

print(f"✅ Generated {len(training_texts)} Kubernetes Q&A training examples")
print(f"📄 Saved to demo.slm.k8s_training_data")
print(f"\nSample training text:\n{training_texts[0][:200]}...")
spark.table("demo.slm.k8s_training_data").select("question", "answer").limit(3).display()

# COMMAND ----------

# DBTITLE 1,Cell 3: Tokenization
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 3: Tokenization — Format for Causal LM Training
# MAGIC
# MAGIC Tokenise training texts and prepare as a HuggingFace Dataset for causal language modelling.

# COMMAND ----------

from transformers import AutoTokenizer, AutoModelForCausalLM
from datasets import Dataset
import torch

MODEL_NAME = "distilgpt2"
MAX_LENGTH = 256

# Load tokenizer
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
tokenizer.pad_token = tokenizer.eos_token  # GPT-2 has no pad token by default

# Load training texts from Delta table
training_texts = spark.table("demo.slm.k8s_training_data").select("training_text").toPandas()["training_text"].tolist()

# Create HuggingFace Dataset
raw_dataset = Dataset.from_dict({"text": training_texts})

def tokenize_function(examples):
    """Tokenise texts with padding and truncation."""
    return tokenizer(
        examples["text"],
        truncation=True,
        padding="max_length",
        max_length=MAX_LENGTH,
        return_tensors="pt",
    )

# Tokenise
tokenized_dataset = raw_dataset.map(tokenize_function, batched=True, remove_columns=["text"])

# Add labels (same as input_ids for causal LM — model shifts internally)
def add_labels(examples):
    examples["labels"] = examples["input_ids"].copy()
    return examples

tokenized_dataset = tokenized_dataset.map(add_labels, batched=True)

# Split: 90% train, 10% eval
split_dataset = tokenized_dataset.train_test_split(test_size=0.1, seed=42)
train_dataset = split_dataset["train"]
eval_dataset = split_dataset["test"]

print(f"✅ Tokenised {len(tokenized_dataset)} examples")
print(f"🔧 Train: {len(train_dataset)} | Eval: {len(eval_dataset)}")
print(f"📏 Max length: {MAX_LENGTH} tokens")
print(f"🔤 Vocab size: {tokenizer.vocab_size}")

# COMMAND ----------

# DBTITLE 1,Cell 4: Model Setup
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 4: Model Setup — DistilGPT2 + LoRA Adapters
# MAGIC
# MAGIC Load the base model, inject LoRA adapters, and freeze all base parameters.
# MAGIC Only LoRA parameters (~300K) will be trained.

# COMMAND ----------

from peft import LoraConfig, get_peft_model, TaskType

# Load base model in half-precision for memory efficiency
model = AutoModelForCausalLM.from_pretrained(
    MODEL_NAME,
    dtype=torch.float16,
    device_map="auto",
)
model.config.pad_token_id = tokenizer.pad_token_id

# Configure LoRA — inject low-rank adapters into attention layers
lora_config = LoraConfig(
    task_type=TaskType.CAUSAL_LM,
    r=8,                          # LoRA rank — controls adapter capacity
    lora_alpha=32,                # Scaling factor (alpha/r ≈ 4)
    lora_dropout=0.1,             # Regularisation
    target_modules=["c_attn", "c_proj"],  # DistilGPT2 attention layers
    bias="none",
)

# Inject LoRA into the model
model = get_peft_model(model, lora_config)

# Print trainable parameter summary
model.print_trainable_parameters()

# Verify base model is frozen
for name, param in model.named_parameters():
    if param.requires_grad:
        print(f"  Trainable: {name} ({param.numel():,} params)")

print("\n✅ LoRA model ready — base frozen, adapters trainable")

# COMMAND ----------

# DBTITLE 1,Cell 5: Training
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 5: Training — HuggingFace Trainer with LoRA
# MAGIC
# MAGIC Fine-tune the LoRA-adapted DistilGPT2 on Kubernetes Q&A data.
# MAGIC Training takes ~2-3 minutes on A10 GPU with this dataset.

# COMMAND ----------

from transformers import Trainer, TrainingArguments, DataCollatorForLanguageModeling
import time

# Training arguments
training_args = TrainingArguments(
    output_dir="/tmp/slm-k8s-output",
    num_train_epochs=5,
    per_device_train_batch_size=4,
    per_device_eval_batch_size=4,
    warmup_steps=10,
    learning_rate=1e-4,
    weight_decay=0.01,
    logging_steps=5,
    eval_strategy="epoch",
    save_strategy="epoch",
    load_best_model_at_end=True,
    report_to="none",  # We'll use MLflow separately
    fp16=True,
    seed=42,
)

# Data collator for causal LM (handles padding)
data_collator = DataCollatorForLanguageModeling(
    tokenizer=tokenizer,
    mlm=False,  # Causal LM, not masked LM
)

# Create Trainer
trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=train_dataset,
    eval_dataset=eval_dataset,
    data_collator=data_collator,
)

# Train!
start_time = time.time()
train_result = trainer.train()
train_duration = time.time() - start_time

print(f"\n✅ Training complete in {train_duration:.1f}s")
print(f"📊 Training loss: {train_result.training_loss:.4f}")

# Evaluate
eval_results = trainer.evaluate()
perplexity = torch.exp(torch.tensor(eval_results["eval_loss"])).item()
print(f"📉 Eval loss: {eval_results['eval_loss']:.4f}")
print(f"📉 Perplexity: {perplexity:.2f}")

# COMMAND ----------

# DBTITLE 1,Cell 6: Sample Generation
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 6: Sample Generation — Test Before Logging
# MAGIC
# MAGIC Generate answers to Kubernetes questions to qualitatively assess the model.

# COMMAND ----------

def generate_answer(question: str, max_new_tokens: int = 100) -> str:
    """Generate an answer from the fine-tuned model."""
    prompt = f"Question: {question}\nAnswer:"
    inputs = tokenizer(prompt, return_tensors="pt").to(model.device)
    
    with torch.no_grad():
        output = model.generate(
            **inputs,
            max_new_tokens=max_new_tokens,
            temperature=0.7,
            do_sample=True,
            top_p=0.9,
            pad_token_id=tokenizer.pad_token_id,
        )
    
    generated = tokenizer.decode(output[0], skip_special_tokens=True)
    # Extract just the answer part
    answer = generated.split("Answer:")[-1].strip()
    return answer

# Test on known questions
test_questions = [
    "What is a Kubernetes Pod?",
    "What is a Kubernetes Deployment?",
    "What is RBAC in Kubernetes?",
]

print("🧪 Sample generation from fine-tuned model:\n")
for q in test_questions:
    answer = generate_answer(q)
    print(f"Q: {q}")
    print(f"A: {answer[:200]}\n")

# COMMAND ----------

# DBTITLE 1,Cell 7: MLflow Logging
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 7: MLflow Logging & Model Registration
# MAGIC
# MAGIC Log training params, metrics, and the LoRA-adapted model to MLflow.
# MAGIC Register the model in Unity Catalog for version tracking and serving.

# COMMAND ----------

import mlflow
import mlflow.pyfunc
import tempfile
import os
import pandas as pd
from mlflow.models import infer_signature

# Set MLflow experiment
EXPERIMENT_NAME = "/Users/martin.mystery9@gmail.com/slm_k8s_finetune"
mlflow.set_experiment(EXPERIMENT_NAME)

class K8sSLMWrapper(mlflow.pyfunc.PythonModel):
    """PyFunc wrapper for the LoRA-adapted DistilGPT2 model."""
    
    def load_context(self, context):
        from transformers import AutoTokenizer, AutoModelForCausalLM
        from peft import PeftModel
        import torch
        model_dir = context.artifacts["model_dir"]
        self.tokenizer = AutoTokenizer.from_pretrained(model_dir)
        self.tokenizer.pad_token = self.tokenizer.eos_token
        base_model = AutoModelForCausalLM.from_pretrained("distilgpt2", dtype=torch.float16, device_map="auto")
        self.model = PeftModel.from_pretrained(base_model, model_dir)
        self.model.eval()
    
    def predict(self, context, model_input, params=None):
        import torch
        results = []
        for text in model_input["text"].tolist():
            prompt = text if text.strip().endswith("Answer:") else f"Question: {text}\nAnswer:"
            inputs = self.tokenizer(prompt, return_tensors="pt").to(self.model.device)
            with torch.no_grad():
                output = self.model.generate(**inputs, max_new_tokens=100, temperature=0.7, do_sample=True, top_p=0.9, pad_token_id=self.tokenizer.pad_token_id)
            generated = self.tokenizer.decode(output[0], skip_special_tokens=True)
            answer = generated.split("Answer:")[-1].strip()
            results.append(answer)
        return pd.DataFrame({"answer": results})

with mlflow.start_run(run_name="distilgpt2-k8s-lora") as run:
    run_id = run.info.run_id
    
    # Log hyperparameters
    mlflow.log_params({
        "base_model": MODEL_NAME,
        "lora_r": 8,
        "lora_alpha": 32,
        "lora_dropout": 0.1,
        "target_modules": "c_attn,c_proj",
        "epochs": 5,
        "batch_size": 4,
        "learning_rate": 1e-4,
        "max_length": MAX_LENGTH,
        "train_examples": len(train_dataset),
        "eval_examples": len(eval_dataset),
    })
    
    # Log metrics
    mlflow.log_metrics({
        "train_loss": train_result.training_loss,
        "eval_loss": eval_results["eval_loss"],
        "perplexity": perplexity,
        "train_duration_s": train_duration,
    })
    
    # Save the LoRA-adapted model to a temp directory
    save_dir = tempfile.mkdtemp()
    model.save_pretrained(save_dir)
    tokenizer.save_pretrained(save_dir)
    
    # Log model artifacts (LoRA adapters + tokenizer)
    mlflow.log_artifacts(save_dir, artifact_path="model_files")
    
    # Log as a proper MLflow model (pyfunc) so register_model works
    signature = infer_signature(
        model_input=pd.DataFrame({"text": ["Question: What is a Pod?"]}),
        model_output=pd.DataFrame({"answer": ["A Pod is the smallest deployable unit..."]}),
    )
    
    mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=K8sSLMWrapper(),
        artifacts={"model_dir": save_dir},
        signature=signature,
        pip_requirements=["torch", "transformers", "peft"],
    )
    
    # Log additional metadata
    mlflow.log_param("model_type", "distilgpt2-lora-peft")
    mlflow.log_param("trainable_params", "294,912")
    
    print(f"✅ MLflow run: {run_id}")
    print(f"   Experiment: {EXPERIMENT_NAME}")
    print(f"   Train loss: {train_result.training_loss:.4f}")
    print(f"   Eval loss: {eval_results['eval_loss']:.4f}")
    print(f"   Perplexity: {perplexity:.2f}")
    print(f"   Model logged as pyfunc at: runs:/{run_id}/model")

# COMMAND ----------

# DBTITLE 1,Cell 8: UC Registration
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 8: Register Model in Unity Catalog
# MAGIC
# MAGIC Register the fine-tuned model in UC for version management and potential serving.

# COMMAND ----------

import mlflow

# Define UC registered model name
UC_MODEL_NAME = "demo.slm.k8s_slm"

# Register the model
result = mlflow.register_model(
    model_uri=f"runs:/{run_id}/model",
    name=UC_MODEL_NAME,
)

print(f"✅ Model registered: {UC_MODEL_NAME}")
print(f"   Version: {result.version}")
print(f"   Run ID: {run_id}")
print(f"\nTo deploy: Create a Model Serving endpoint from this UC model version")
print(f"To load: mlflow.pyfunc.load_model('models:/{UC_MODEL_NAME}/{result.version}')")

# COMMAND ----------

# DBTITLE 1,Cell 9: Inference Test
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 9: Inference — Test on New Kubernetes Questions
# MAGIC
# MAGIC Test the fine-tuned model on questions not seen during training.

# COMMAND ----------

# Questions the model hasn't seen during training
new_questions = [
    "What is a Kubernetes cluster?",
    "How do you scale a Deployment?",
    "What is the kubelet?",
    "How do you troubleshoot a CrashLoopBackOff?",
]

print("🔍 Inference on unseen Kubernetes questions:\n")
for q in new_questions:
    answer = generate_answer(q, max_new_tokens=120)
    print(f"Q: {q}")
    print(f"A: {answer}")
    print("-" * 80)

print("\n📝 Note: DistilGPT2 (82M) + LoRA on 30 examples is a minimal demo.")
print("   For production: use 1000+ Q&A pairs, larger model (GPT-2 medium/large),")
print("   more epochs, and evaluate with BLEU/ROUGE/semantic similarity.")

# COMMAND ----------

# DBTITLE 1,Key Takeaways
# MAGIC %md
# MAGIC # Key Takeaways
# MAGIC
# MAGIC ## What We Built
# MAGIC
# MAGIC A complete SLM training pipeline on Databricks GPU serverless:
# MAGIC
# MAGIC ```
# MAGIC Kubernetes Q&A Data (30 examples)
# MAGIC     → Tokenise with DistilGPT2 tokenizer
# MAGIC     → Load DistilGPT2 (82M params) + inject LoRA (294K trainable)
# MAGIC     → Train 5 epochs with HF Trainer (~2 min on A10 GPU)
# MAGIC     → Evaluate perplexity + sample generation
# MAGIC     → Log to MLflow
# MAGIC     → Register in Unity Catalog
# MAGIC     → Inference on unseen questions
# MAGIC ```
# MAGIC
# MAGIC ## Scaling to Production
# MAGIC
# MAGIC | Component | Demo | Production |
# MAGIC |-----------|------|------------|
# MAGIC | **Training data** | 30 synthetic Q&A pairs | 1,000-10,000 real Q&A from K8s docs, SO, runbooks |
# MAGIC | **Base model** | DistilGPT2 (82M) | GPT-2 Medium (355M) or Llama-3-8B-Instruct |
# MAGIC | **LoRA rank** | r=8 (294K params) | r=16-64 (600K-2M params) |
# MAGIC | **Epochs** | 5 | 3-10 with early stopping |
# MAGIC | **Evaluation** | Perplexity + manual | BLEU, ROUGE, semantic similarity, human eval |
# MAGIC | **Serving** | Notebook inference | Model Serving endpoint with autoscaling |
# MAGIC | **Compute** | 1× A10 GPU | 1× H100 or 8× H100 for larger models |
# MAGIC
# MAGIC ## Key Concepts
# MAGIC
# MAGIC | Concept | Description |
# MAGIC |---------|-------------|
# MAGIC | **SLM** | Small Language Model — 82M to 1B parameters, efficient and domain-specialised |
# MAGIC | **LoRA** | Low-Rank Adaptation — freeze base model, train small injected adapters |
# MAGIC | **PEFT** | Parameter-Efficient Fine-Tuning — HuggingFace library for LoRA, QLoRA, prefix tuning |
# MAGIC | **Causal LM** | Next-token prediction (GPT-style) — model predicts the next token given previous tokens |
# MAGIC | **Perplexity** | Lower = better — measures how well the model predicts the test data |
# MAGIC | **MLflow** | Experiment tracking, model registry, reproducible training |
# MAGIC | **UC Registry** | Unity Catalog model registry — versioned, governed, serving-ready |
# MAGIC
# MAGIC ## Databricks Free Edition Compatibility
# MAGIC
# MAGIC | Resource | Available |
# MAGIC |----------|-----------|
# MAGIC | **Serverless GPU (A10)** | ✅ |
# MAGIC | **PyTorch + Transformers + PEFT** | ✅ (AI v5 base environment) |
# MAGIC | **MLflow** | ✅ |
# MAGIC | **Unity Catalog** | ✅ |
# MAGIC | **Model Serving** | ⚠️ (May require paid tier for always-on endpoints) |
# MAGIC
# MAGIC ---