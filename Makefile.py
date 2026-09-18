# =============================================================================
# Makefile — Mac M3 Pro Local Development with Databricks Connect
# =============================================================================
# This Makefile automates setup, configuration, and testing of a local
# development environment on Mac M3 Pro that connects to Databricks
# serverless compute via Databricks Connect (Spark Connect protocol).
#
# Usage:
#   make help          — Show all available commands
#   make setup        — Create venv and install all packages
#   make configure     — Interactive: set workspace URL and token in ~/.databrickscfg
#   make test          — Run the full 6-step pipeline test
#   make test-spark    — Test Spark connection only
#   make test-pytorch  — Test PyTorch MPS (Apple Silicon GPU)
#   make test-mlflow   — Test MLflow tracking
#   make test-sdk      — Test Databricks SDK (workspace API)
#   make clean         — Remove venv and test artifacts
#   make info          — Show current environment info
# =============================================================================

# --- Configuration ----------------------------------------------------------
PYTHON        := python3.12
VENV_DIR      := $(HOME)/databricks-env
VENV_PYTHON   := $(VENV_DIR)/bin/python3
VENV_PIP      := $(VENV_DIR)/bin/pip
SCRIPT        := scripts/test_databricks_connect.py
CFG_FILE      := $(HOME)/.databrickscfg

# Serverless compute flag (required for Databricks Connect)
export DATABRICKS_CONNECT_SERVERLESS := 1

# Packages to install in the venv
PACKAGES := databricks-connect databricks-sdk mlflow scikit-learn pandas numpy matplotlib torch

# --- Default target ---------------------------------------------------------
.PHONY: help
help:
	@echo ""
	@echo "Mac M3 Pro — Databricks Connect Setup"
	@echo "======================================"
	@echo ""
	@echo "Available commands:"
	@echo ""
	@echo "  make setup         Create ~/databricks-env and install packages"
	@echo "  make configure     Set workspace URL and token in ~/.databrickscfg"
	@echo "  make test          Run full 6-step pipeline test"
	@echo "  make test-spark    Test Spark connection to serverless"
	@echo "  make test-pytorch  Test PyTorch MPS (Apple Silicon GPU)"
	@echo "  make test-mlflow   Test MLflow experiment tracking"
	@echo "  make test-sdk      Test Databricks SDK (workspace API)"
	@echo "  make info          Show current environment info"
	@echo "  make clean         Remove venv and test artifacts"
	@echo "  make help          Show this help message"
	@echo ""
	@echo "Quick start:"
	@echo "  make setup && make configure && make test"
	@echo ""

# --- Setup ------------------------------------------------------------------
.PHONY: setup
setup:
	@echo ""
	@echo "=== Setting up ~/databricks-env ==="
	@echo ""
	# Create venv (skip if already exists)
	@if [ -d "$(VENV_DIR)" ]; then \
		echo "  venv already exists at $(VENV_DIR)"; \
	else \
		echo "  Creating venv with $(PYTHON)..."; \
		$(PYTHON) -m venv $(VENV_DIR); \
		echo "  venv created"; \
	fi
	@echo ""
	@echo "  Upgrading pip..."
	@$(VENV_PIP) install --upgrade pip
	@echo ""
	@echo "  Installing packages: $(PACKAGES)"
	@$(VENV_PIP) install $(PACKAGES)
	@echo ""
	@echo "  Verifying installation..."
	@$(VENV_PYTHON) -c "import databricks.connect; import databricks.sdk; import mlflow; import sklearn; import torch; print('  All packages installed successfully')"
	@echo ""
	@echo "=== Setup complete ==="
	@echo ""
	@echo "Next: make configure"

# --- Configure --------------------------------------------------------------
.PHONY: configure
configure:
	@echo ""
	@echo "=== Configure ~/.databrickscfg ==="
	@echo ""
	@if [ -f "$(CFG_FILE)" ]; then \
		echo "  Existing config found at $(CFG_FILE)"; \
		echo "  Current contents:"; \
		echo "  ---"; \
		grep -v 'token' $(CFG_FILE) | sed 's/^/  /'; \
		echo "  token = <hidden>"; \
		echo "  ---"; \
		echo ""; \
		read -p "  Overwrite? (y/N): " overwrite; \
		if [ "$$overwrite" != "y" ] && [ "$$overwrite" != "Y" ]; then \
			echo "  Keeping existing config."; \
			echo ""; \
			exit 0; \
		fi; \
	fi
	@echo "  Enter your Databricks workspace URL (e.g. https://dbc-xxxx.cloud.databricks.com):"
	@read -p "  Workspace URL: " workspace_url; \
	read -sp "  Personal Access Token: " token; echo ""; \
	printf "[DEFAULT]\nhost  = %s\ntoken = %s\n" "$$workspace_url" "$$token" > $(CFG_FILE); \
	chmod 600 $(CFG_FILE); \
	echo "  Config written to $(CFG_FILE)"; \
	echo "  Permissions set to 600 (owner read/write only)"
	@echo ""
	@echo "=== Configuration complete ==="
	@echo ""
	@echo "Next: make test"

# --- Test targets -----------------------------------------------------------
.PHONY: test
test:
	@echo ""
	@echo "=== Running full pipeline test ==="
	@echo ""
	@$(VENV_PYTHON) $(SCRIPT)

.PHONY: test-spark
test-spark:
	@echo ""
	@echo "=== Testing Spark connection ==="
	@echo ""
	@$(VENV_PYTHON) -c "
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.serverless(True).getOrCreate()
result = spark.sql('SELECT 1 AS test_value').collect()
assert result[0]['test_value'] == 1
print('Spark connection: OK')
print('Result:', result)
"

.PHONY: test-pytorch
test-pytorch:
	@echo ""
	@echo "=== Testing PyTorch MPS (Apple Silicon GPU) ==="
	@echo ""
	@$(VENV_PYTHON) -c "
import torch
mps = torch.backends.mps.is_available()
print(f'PyTorch: {torch.__version__}')
print(f'MPS available: {mps}')
if mps:
    device = torch.device('mps')
    x = torch.randn(3, 3).to(device)
    y = torch.randn(3, 3).to(device)
    z = torch.mm(x, y)
    print(f'Matrix mult on MPS: {z.shape}')
    print(f'Sample: {z[0,0].item():.4f}')
    print('PyTorch MPS: OK')
else:
    print('MPS not available, CPU fallback works')
"

.PHONY: test-mlflow
test-mlflow:
	@echo ""
	@echo "=== Testing MLflow tracking ==="
	@echo ""
	@$(VENV_PYTHON) -c "
import mlflow
import mlflow.sklearn
from sklearn.linear_model import LogisticRegression
from sklearn.datasets import make_classification

mlflow.set_experiment('makefile-test')
with mlflow.start_run(run_name='mac_test') as run:
    X, y = make_classification(n_samples=100, n_features=5, random_state=42)
    model = LogisticRegression(max_iter=100)
    model.fit(X, y)
    acc = model.score(X, y)
    mlflow.log_metric('accuracy', acc)
    mlflow.sklearn.log_model(model, 'model')
    print(f'MLflow run: {run.info.run_id}')
    print(f'Accuracy: {acc:.4f}')
    print('MLflow: OK')
"

.PHONY: test-sdk
test-sdk:
	@echo ""
	@echo "=== Testing Databricks SDK ==="
	@echo ""
	@$(VENV_PYTHON) -c "
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
me = w.current_user.me()
print(f'Authenticated as: {me.user_name}')
catalogs = list(w.catalogs.list())
print(f'Catalogs accessible: {len(catalogs)}')
for c in catalogs[:5]:
    print(f'  - {c.name}')
print('Databricks SDK: OK')
"

# --- Info -------------------------------------------------------------------
.PHONY: info
info:
	@echo ""
	@echo "=== Environment Info ==="
	@echo ""
	@echo "  Python:   $(PYTHON)"
	@echo "  Venv:     $(VENV_DIR)"
	@echo "  Script:   $(SCRIPT)"
	@echo "  Config:   $(CFG_FILE)"
	@echo "  Serverless: $$DATABRICKS_CONNECT_SERVERLESS"
	@echo ""
	@if [ -d "$(VENV_DIR)" ]; then \
		echo "  Venv status: created"; \
		echo "  Python:  $$( $(VENV_PYTHON) --version 2>&1 )"; \
		echo "  Packages:"; \
		$(VENV_PIP) list --format=columns | grep -E 'databricks|mlflow|scikit|torch|pandas' | sed 's/^/    /'; \
	else \
		echo "  Venv status: NOT created (run: make setup)"; \
	fi
	@echo ""
	@if [ -f "$(CFG_FILE)" ]; then \
		echo "  Config: exists"; \
		echo "    host:  $$( grep '^host' $(CFG_FILE) | sed 's/host *= *//' )"; \
		echo "    token: <hidden>"; \
	else \
		echo "  Config: NOT found (run: make configure)"; \
	fi
	@echo ""

# --- Clean ------------------------------------------------------------------
.PHONY: clean
clean:
	@echo ""
	@echo "=== Cleaning up ==="
	@echo ""
	rm -rf $(VENV_DIR)
	rm -rf mlruns/
	@if [ -f "$(CFG_FILE)" ]; then \
		echo "  ~/.databrickscfg kept (remove manually if needed: rm $(CFG_FILE))"; \
	fi
	@echo ""
	@echo "  Done. Venv and MLflow runs removed."
	@echo ""

# --- Connection quick test --------------------------------------------------
.PHONY: quick-test
quick-test:
	@echo ""
	@echo "=== Quick connection test ==="
	@echo ""
	@$(VENV_PYTHON) -c "
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.serverless(True).getOrCreate()
print(spark.sql('SELECT 1').collect())
print('Connection OK!')
"
