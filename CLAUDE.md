# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

OSLM-Analyst (Open-source large models data analyst) is a data collection and analysis pipeline for tracking open-source AI models and datasets across multiple platforms (HuggingFace, ModelScope, BAAI DataHub). It crawls metadata like download counts, likes, and uses AI to classify models/datasets by modality and lifecycle.

## Key Architecture

The pipeline follows this flow: **Configuration → Crawling → Enrichment → Output → Analysis**

### Main Components

| Component | Path | Purpose |
|-----------|------|---------|
| CLI | `src/oslm_analyst/cli.py` | Typer-based command-line interface |
| Crawl Orchestrator | `src/oslm_analyst/crawl.py` | Manages the crawling pipeline |
| HuggingFace Crawler | `src/oslm_analyst/crawlers/huggingface.py` | HF Hub API integration |
| ModelScope Crawler | `src/oslm_analyst/crawlers/modelscope.py` | ModelScope API integration |
| BAAI Crawler | `src/oslm_analyst/crawlers/baai_data.py` | BAAI DataHub web scraper |
| Modality Processor | `src/oslm_analyst/processors/modality.py` | AI-powered classification via LangChain |
| Data Structures | `src/oslm_analyst/data_utils.py` | Enums (Modality, Lifecycle) and dataclasses |
| OSIR-LMTS Database | `src/oslm_analyst/database/osir_lmts.py` | SQLite database for historical data |
| MCP Server | `src/oslm_analyst/mcp_server.py` | MCP server for safe database access |

## Setup & Commands

**Package Manager:** uv (uses uv.build)

```bash
# Install dependencies
uv sync

# Run CLI
uv run oslm-analyst --help

# Run tests
uv run pytest tests/ -v
```

### Common CLI Commands

```bash
# Crawl all models from HuggingFace using config
uv run oslm-analyst crawl huggingface ./config/orgs.yaml --category model

# Crawl a single model by ID
uv run oslm-analyst crawl huggingface id:deepseek-ai/DeepSeek-V3.2-Speciale --category model

# Post-process to add modality/lifecycle classification
uv run oslm-analyst process gen-modality ./output/huggingface_2026-03-17

# Process OSIR-LMTS aggregated data
uv run oslm-analyst process osir-lmts

# Database commands
uv run oslm-analyst db init                    # Initialize database from all osir-lmts directories
uv run oslm-analyst db update ./output/osir-lmts_2026-03  # Update with specific month
uv run oslm-analyst db list                    # List available months
uv run oslm-analyst db mcp                     # Start MCP server
```

## Configuration

- `config/orgs.yaml` - List of organizations to track (with hf_accounts, ms_accounts)
- `config/model_info.jsonl` - Persistent model extra info (modality, validity)
- `config/dataset_info.jsonl` - Persistent dataset extra info (modality, lifecycle, validity)
- `config/osir_lmts_orgs.json` - List of organizations for OSIR-LMTS ranking

## Output Format

Raw output is stored as JSONLines in `output/{platform}_{YYYY-MM-DD}/`:
- `raw_{category}_data.jsonl` - Crawled data
- `err_{category}_data.jsonl` - Errors (deleted if none)

OSIR-LMTS output is in `output/osir-lmts_{YYYY-MM}/`:
- `model_data.jsonl` / `dataset_data.jsonl` - Monthly delta data
- `acc_model_data.jsonl` / `acc_dataset_data.jsonl` - Accumulated data
- `*_summary.csv` - Summary statistics
- `*_rank.csv` - Ranking results

## Database

The OSIR-LMTS SQLite database (`output/osir_lmts.db`) stores historical model and dataset data:

- `models` table: Monthly model metrics (downloads, likes, modality, etc.)
- `datasets` table: Monthly dataset metrics (downloads, likes, modality, lifecycle, etc.)

## MCP Server & Data Analysis Skill

**MCP Server**: Provides safe read-only access to the database via Model Context Protocol. Configured in `.mcp.json`.

**Data Analysis Skill**: The `osir-lmts-analyst` skill (`.claude/skills/osir-lmts-analyst/`) uses the MCP server to:
- Query download trends and organization rankings
- Generate visualizations (line charts, pie charts, bar charts, radar charts)
- Analyze monthly changes

Use this skill when users ask about OSIR-LMTS data analysis.

## Key Design Patterns

- **Retry with backoff**: Uses `tenacity` with automatic wait time extraction from API headers
- **Atomic updates**: Writes to temp file first, then atomically replaces to prevent corruption
- **Persistent caching**: Extra info cached in `config/` to avoid reprocessing
- **JSONLines streaming**: Handles large datasets without loading everything into memory

## Python Version

Requires Python >= 3.12