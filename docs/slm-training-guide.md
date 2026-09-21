# Small Language Model Training Guide

Fine-tune a domain-specific SLM on Databricks GPU serverless using LoRA/PEFT — end-to-end from data to inference.

## Architecture

```
Domain Q&A Data -> Tokenise (HF) -> Base Model + LoRA (PEFT) -> Train (HF Trainer) -> MLflow + UC Registry -> Inference
```

## Key Concepts

| Concept | Description |
|---------|-------------|
| **SLM** | Small Language Model — 82M to 1B parameters. Efficient, domain-specialised, runs on single GPU. |
| **LoRA** | Low-Rank Adaptation — freezes base model weights, injects small trainable rank-decomposed matrices into attention layers. Only ~0.5% of parameters are trained. |
| **PEFT** | Parameter-Efficient Fine-Tuning — HuggingFace library implementing LoRA, QLoRA, prefix tuning. |
| **Causal LM** | Next-token prediction (GPT-style). Model predicts the next token given previous tokens. |
| **Perplexity** | Exponentiated cross-entropy loss. Lower = better. Measures how well the model predicts test data. |
| **PyFunc Model** | MLflow's universal model interface. Wraps any model (including PEFT) for reproducible inference. |

## Why LoRA Instead of Full Fine-Tuning?

| Approach | Trainable Params | GPU Memory | Training Time | Quality |
|----------|----------------|------------|---------------|---------|
| Full fine-tuning | 82M (100%) | High | Slow | Baseline |
| LoRA (r=8) | ~405K (0.5%) | Low | Fast | Near-identical |
| QLoRA (4-bit + LoRA) | ~405K (0.5%) | Very low | Fast | Slight degradation |

LoRA trains small adapter matrices injected into the frozen base model's attention layers. At inference, adapters merge with base weights — no additional latency.

## Databricks Free Edition Compatibility

| Resource | Available | Notes |
|----------|-----------|-------|
| Serverless GPU (A10) | Yes | AI v5 base environment pre-installs torch, transformers, PEFT |
| PyTorch | Yes | v2.9.0+cu129 |
| HuggingFace Transformers | Yes | v4.57.1 |
| PEFT (LoRA) | Yes | v0.17.1 |
| MLflow | Yes | Experiment tracking + model registry |
| Unity Catalog | Yes | Model registration as demo.slm.k8s_slm |
| Model Serving | Limited | May require paid tier for always-on endpoints |

## Pipeline Stages

### 1. Data Preparation

Generate or collect domain-specific Q&A pairs. For this demo, 30 Kubernetes Q&A pairs are synthetically generated and saved to a Delta table (demo.slm.k8s_training_data).

**Production**: Use 1,000-10,000 real Q&A pairs from:
- Kubernetes official documentation
- Stack Overflow questions + accepted answers
- Internal runbooks and troubleshooting guides
- Community Slack/Discord archives

### 2. Tokenization

- Load DistilGPT2 tokenizer (GPT-2 BPE, vocab size 50,257)
- Set pad_token = eos_token (GPT-2 has no pad token by default)
- Tokenise with truncation (max_length=256) and padding
- Add labels = input_ids (causal LM shifts internally)
- Split 90% train / 10% eval

### 3. Model Setup (LoRA)

Load DistilGPT2 in half-precision, inject LoRA adapters (r=8, alpha=32, dropout=0.1) into c_attn and c_proj layers. Result: 405,504 trainable parameters (0.49% of 82M total).

### 4. Training

- HuggingFace Trainer with TrainingArguments
- 5 epochs, batch size 4, learning rate 1e-4, fp16
- eval_strategy='epoch' for per-epoch evaluation
- load_best_model_at_end=True for best checkpoint
- Training time: ~14 seconds on A10 GPU

### 5. Evaluation

| Metric | Value | Interpretation |
|--------|-------|---------------|
| Training loss | 4.1640 | Decreasing — model is learning |
| Eval loss | 3.7232 | Lower than train — no overfitting |
| Perplexity | 41.40 | Lower = better. Baseline GPT-2 perplexity ~50-100 |

### 6. MLflow Logging

- Log all hyperparameters (LoRA r, alpha, dropout, learning rate, epochs)
- Log metrics (train loss, eval loss, perplexity, training duration)
- Log model as mlflow.pyfunc with a custom K8sSLMWrapper class
- The wrapper handles tokenisation + generation at inference time

### 7. UC Registration

Register the model in Unity Catalog as demo.slm.k8s_slm. The model can be loaded via:
```python
model = mlflow.pyfunc.load_model('models:/demo.slm.k8s_slm/1')
```

### 8. Inference

The fine-tuned model generates K8s-domain text. With only 30 training examples, the output is repetitive but shows learned Kubernetes vocabulary. For production quality:
- Use 1,000+ Q&A pairs
- Larger base model (GPT-2 Medium 355M or Llama-3-8B-Instruct)
- More LoRA rank (r=16-64)
- BLEU/ROUGE/semantic similarity evaluation

## Scaling to Production

| Dimension | Demo | Production |
|-----------|------|------------|
| Training data | 30 synthetic Q&A | 1,000-10,000 real Q&A |
| Base model | DistilGPT2 (82M) | GPT-2 Medium (355M) / Llama-3-8B |
| LoRA rank | r=8 (405K params) | r=16-64 (600K-2M params) |
| Epochs | 5 | 3-10 with early stopping |
| Evaluation | Perplexity + manual | BLEU, ROUGE, semantic similarity |
| Serving | Notebook inference | Model Serving endpoint |
| Compute | 1x A10 GPU | 1x H100 or 8x H100 |
| Monitoring | None | Drift detection, response quality |

## Compute Requirements

This notebook requires **Serverless GPU compute** with the **AI v5 base environment**, which pre-installs:
- PyTorch 2.9.0+ (CUDA 12.9)
- HuggingFace Transformers 4.57.1
- PEFT 0.17.1
- Datasets, Accelerate, MLflow

Do NOT pip install these GPU libraries on CPU compute — they require CUDA and will crash the kernel.

## References

- [HuggingFace Transformers](https://huggingface.co/docs/transformers/index)
- [PEFT (LoRA)](https://huggingface.co/docs/peft/index)
- [DistilGPT2 Model](https://huggingface.co/distilgpt2)
- [LoRA Paper](https://arxiv.org/abs/2106.09685)
- [Databricks GPU Compute](https://docs.databricks.com/aws/en/compute/gpu/)
- [MLflow Model Registry](https://mlflow.org/docs/latest/model-registry.html)