# Databricks notebook source
# DBTITLE 1,Git Integration Overview
# MAGIC %md
# MAGIC # Git Integration on Databricks
# MAGIC
# MAGIC **Objective**: Learn how to use Git folders (Repos) in Databricks for version control, branch management, CI/CD integration, and collaborative development.
# MAGIC
# MAGIC ## Git Integration Architecture
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart LR
# MAGIC     subgraph "External Git Provider"
# MAGIC         G[GitHub/GitLab/Bitbucket] --> R[Remote Repository]
# MAGIC     end
# MAGIC     subgraph "Databricks Workspace"
# MAGIC         R -->|clone/pull| F[Git Folder /Repos/user/repo]
# MAGIC         F -->|commit/push| R
# MAGIC         F --> N[Notebooks]
# MAGIC         F --> P[Pipelines]
# MAGIC         F --> J[Jobs]
# MAGIC     end
# MAGIC     subgraph "CI/CD"
# MAGIC         R -->|webhook| CI[GitHub Actions] -->|DAB deploy| DB[Databricks]
# MAGIC     end
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Cell 1: Git Folder Setup
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 1: Git Folder Setup and Management

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

print('Git Folder Management:')
print()
print('1. Create a Git folder (clone a repo):')
print('   Via UI: Workspace > Repos > Add Repo')
print('   Via SDK:')
print('   w.repos.create(url="https://github.com/user/repo.git", provider="github", path="/Repos/user/repo")')
print()
print('2. List Git folders:')
for repo in w.repos.list():
    print(f'   Repo: {repo.path} | Provider: {repo.provider} | Branch: {repo.branch}')
print()
print('3. Pull latest changes:')
print('   w.repos.update(repo_id, branch="main")')
print()
print('4. Switch branch:')
print('   w.repos.update(repo_id, branch="feature-branch")')
print()
print('5. Get repo info:')
print('   repo_info = w.repos.get(repo_id)')
print()
print('Supported Git Providers:')
print('   - GitHub (github.com)')
print('   - GitLab (gitlab.com or self-hosted)')
print('   - Bitbucket (bitbucket.org)')
print('   - Azure DevOps (dev.azure.com)')
print('   - AWS CodeCommit')
print('   - Any Git server with HTTPS access')

# COMMAND ----------

# DBTITLE 1,Cell 2: Branch Management
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 2: Branch Management and Workflows

# COMMAND ----------

print('Git Branch Workflows on Databricks:')
print()
print('=== Gitflow Workflow ===')
print('  main          - production code (protected)')
print('  develop       - integration branch')
print('  feature/*     - feature branches from develop')
print('  hotfix/*      - urgent fixes from main')
print('  release/*     - release preparation')
print()
print('=== Trunk-Based Development ===')
print('  main          - single integration branch')
print('  feature/*     - short-lived branches, merge fast')
print()
print('Branch Operations via SDK:')
print()
print('# List branches')
print('  branches = w.repos.get_branches(repo_id)')
print()
print('# Switch to a branch')
print('  w.repos.update(repo_id, branch="feature/new-pipeline")')
print()
print('# Create a branch (via Git API)')
print('  w.repos.create_branch(repo_id, branch="feature/ml-model", ref="main")')
print()
print('# Delete a branch')
print('  w.repos.delete_branch(repo_id, branch="feature/old-experiment")')
print()
print('Best Practices:')
print('   - One Git folder per user (auto-created on clone)')
print('   - Use DAB (databricks.yml) for environment-specific configs')
print('   - Use branch protection rules on the Git provider')
print('   - Pull before starting work to avoid conflicts')
print('   - Commit and push from Databricks notebook UI or CLI')

# COMMAND ----------

# DBTITLE 1,Cell 3: Git Operations
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 3: Git Operations via CLI and API

# COMMAND ----------

print('Git Operations on Databricks:')
print()
print('=== Databricks CLI ===')
print('  databricks repos list                           # List all Git folders')
print('  databricks repos get --repo-id <id>              # Get repo details')
print('  databricks repos update --repo-id <id> --branch main  # Pull/switch branch')
print()
print('=== Git CLI (if enabled in workspace) ===')
print('  cd /Workspace/Repos/user/repo')
print('  git status                                      # Check working tree')
print('  git add .                                        # Stage changes')
print('  git commit -m "Add new pipeline module"        # Commit')
print('  git push origin feature/new-module              # Push to remote')
print('  git pull origin main                            # Pull latest')
print('  git checkout -b feature/new-module              # Create and switch branch')
print('  git log --oneline -5                            # Recent commits')
print()
print('=== Databricks SDK (Python API) ===')
print('  from databricks.sdk import WorkspaceClient')
print('  w = WorkspaceClient()')
print()
print('  # Get all repos')
print('  for repo in w.repos.list():')
print('      print(f"{repo.path} | {repo.url} | {repo.branch}")')
print()
print('  # Pull latest')
print('  w.repos.update(repo_id=repo_id, branch="main")')
print()
print('=== Commit from Notebook ===')
print('  In notebook: File > Source Control > Commit')
print('  Or use Git CLI if enabled')
print('  Changes are committed to the current branch')
print('  Push to sync with remote repository')

# COMMAND ----------

# DBTITLE 1,Cell 4: CI/CD Pipeline
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 4: CI/CD Pipeline with Git and DAB

# COMMAND ----------

print('CI/CD Pipeline on Databricks with Git:')
print()
print('flowchart: Developer commits -> Git Push -> GitHub Actions -> validate -> deploy dev -> test -> deploy staging -> approve -> deploy prod')
print()
print('Pipeline Components:')
print('   1. Source: Git folder with DAB (databricks.yml)')
print('   2. CI: GitHub Actions / GitLab CI / Azure Pipelines')
print('   3. CD: Databricks CLI bundle deploy command')
print('   4. Environments: dev, staging, prod targets in DAB')
print('   5. Secrets: Databricks tokens as CI secrets')
print('   6. Notifications: Slack/email on success or failure')
print()
print('Example GitHub Actions step:')
print('   - name: Deploy to Databricks')
print('     run: |')
print('       pip install databricks-sdk')
print('       databricks bundle validate')
print('       databricks bundle deploy -t dev')
print('     env:')
print('       DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}')
print('       DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}')
print()
print('This repository itself uses Git integration:')
print('   Path: /Repos/martin.mystery9@gmail.com/dataengineering')
print('   Provider: GitHub')
print('   Branch: main')

# COMMAND ----------

