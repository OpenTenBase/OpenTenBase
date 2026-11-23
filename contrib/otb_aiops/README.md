# otb_aiops

`otb_aiops` is a lightweight C++17 CLI diagnostic and AIOps tool for OpenTenBase.
It runs strictly read-only inspections against OpenTenBase clusters (via `libpq`) and optionally sends structured diagnostic snapshots to any OpenAI-compatible LLM endpoint (via `libcurl`) for root-cause analysis.

## Build & Install

Requires C++17 compiler, OpenTenBase `libpq`, and `libcurl`:

```bash
make -C contrib/otb_aiops
make -C contrib/otb_aiops install   # installs to ~/.local/bin/otb_aiops by default
```

## Quick Start

Run the interactive setup wizard once to choose your AI provider (DeepSeek, OpenAI, Qwen DashScope, Ollama, or custom), model, API key, and default database connection (saved to `~/.config/otb_aiops/config.ini` with mode `0600`):

```bash
otb_aiops config
otb_aiops config show
```

Configuration precedence: **CLI flags > Environment variables > `~/.config/otb_aiops/config.ini` > defaults**.

## Usage

### 1. Deterministic Diagnostics (No LLM required)

```bash
# Cluster health check (nodes, groups, connections, replication)
otb_aiops -h 127.0.0.1 -p 11003 -U opentenbase -d postgres health

# JSON output for automation / pipelines
otb_aiops -p 11003 health --json

# Inspect locks & long-running transactions
otb_aiops -p 11003 inspect locks

# Inspect dangling 2PC transactions
otb_aiops -p 11003 inspect 2pc

# Inspect top active / slow queries
otb_aiops -p 11003 inspect slow-sql --top 10

# Inspect key GUC parameters
otb_aiops -p 11003 inspect guc

# Inspect largest relations and table bloat / dead tuples
otb_aiops -p 11003 inspect storage --top 10
```

### 2. AIOps Root-Cause Diagnosis & Q&A

Configure an OpenAI-compatible endpoint via environment variables (or `--api-url`, `--api-key`, `--model`):

```bash
export OTB_AI_BASE_URL="https://api.openai.com/v1/chat/completions"
export OTB_AI_API_KEY="sk-..."
export OTB_AI_MODEL="gpt-4o-mini"

otb_aiops -h 127.0.0.1 -p 11003 -U opentenbase -d postgres diagnose
otb_aiops -h 127.0.0.1 -p 11003 -U opentenbase -d postgres ask "Why are writes slowing down?"
```

## Tests

```bash
make -C contrib/otb_aiops test
```

## Safety

- Every database session enforces `SET default_transaction_read_only = on` and `SET statement_timeout` (default `5000ms`).
- `diagnose` and `ask` only recommend read-safe verification SQL / low-impact remediation and never execute mutating statements automatically.
- User configuration is stored at `~/.config/otb_aiops/config.ini` with POSIX `0600` permissions; API keys are hidden during input and masked on display.

## Scope & Roadmap

- **Current scope (v0.1.0)**: Connects to a single OpenTenBase node (Coordinator or Datanode) via `libpq`, inspecting local runtime views (`pg_stat_activity`, `pg_locks`, `pg_prepared_xacts`, `pg_settings`, `pg_stat_user_tables`) together with OpenTenBase distributed catalog metadata (`pgxc_node`, `pgxc_group`).
- **Planned follow-up**: Cluster-wide fan-out inspection (`--all-nodes`) across all CN/DN nodes registered in `pgxc_node` without requiring SSH.
