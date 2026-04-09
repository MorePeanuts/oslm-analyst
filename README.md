# OSLM-Analyst

Open-source large models data analyst.

## Overview

OSLM-Analyst is a data collection and analysis pipeline for tracking open-source AI models and datasets across multiple platforms (HuggingFace, ModelScope, BAAI DataHub). It crawls metadata like download counts, likes, and uses AI to classify models/datasets by modality and lifecycle.

The project aims to analyze hotspots and development trends in the open-source large model domain.

## Features

### Completed
- **Data Crawling**: Multi-platform support for HuggingFace, ModelScope, and BAAI DataHub
- **Data Statistics Tools**: Aggregation and processing of crawled data
- **AI-assisted Modality Generation**: Automatic classification of models and datasets using LLMs
- **Streamlit Dashboard**: Interactive data visualization interface
- **OSIR-LMTS Database**: SQLite database for storing historical model/dataset data
- **MCP Server**: Model Context Protocol server for safe database access
- **Data Analysis Skill**: Claude Code skill for interactive data analysis

### In Progress / Planned
- **Data Analysis Agent**: AI-powered agent for answering questions about the data

## Installation

```bash
# Install dependencies using uv
uv sync
```

## Usage

### CLI Commands

```bash
# View all commands
uv run oslm-analyst --help

# Crawl data from platforms
uv run oslm-analyst crawl huggingface --category model
uv run oslm-analyst crawl huggingface --category dataset
uv run oslm-analyst crawl modelscope --category model
uv run oslm-analyst crawl modelscope --category dataset
uv run oslm-analyst crawl baai-datahub

# Generate modality information
uv run oslm-analyst process gen-modality output/huggingface_YYYY-MM-DD
uv run oslm-analyst process gen-modality output/modelscope_YYYY-MM-DD
uv run oslm-analyst process gen-modality output/baai-datahub_YYYY-MM-DD

# Process OSIR-LMTS data
uv run oslm-analyst process osir-lmts

# Database management
uv run oslm-analyst db init                    # Initialize database from all osir-lmts directories
uv run oslm-analyst db update ./output/osir-lmts_YYYY-MM  # Update database with specific month
uv run oslm-analyst db list                    # List available months in database
uv run oslm-analyst db mcp                     # Start MCP server for database access

# Launch dashboard
uv run oslm-analyst dashboard
```

## Project Structure

```
OSLM-Analyst/
├── .claude/skills/         # Claude Code skills
│   └── osir-lmts-analyst/  # OSIR-LMTS data analysis skill
├── .mcp.json               # MCP server configuration
├── config/                 # Configuration files
├── output/                 # Output data directory
├── osir_lmts/              # OSIR-LMTS specific files
├── scripts/                # Utility scripts
├── src/oslm_analyst/       # Main source code
│   ├── crawl.py            # Crawl orchestrator
│   ├── crawlers/           # Platform-specific crawlers
│   ├── processors/         # Data processors
│   ├── database/           # Database module
│   │   └── osir_lmts.py   # OSIR-LMTS database implementation
│   ├── ui/                 # Streamlit dashboard
│   ├── mcp_server.py       # MCP server implementation
│   └── cli.py              # CLI entrypoint
└── tests/                  # Tests
```

## OSIR-LMTS Database

The project includes an SQLite database (`output/osir_lmts.db`) for storing historical model and dataset data.

### Database Schema

**`models` table**
- `id` (TEXT): Full identifier (org/model-name, primary key)
- `month` (TEXT): Month in YYYY-MM format (primary key)
- `org` (TEXT): Organization name
- `repo` (TEXT): Repository name
- `name` (TEXT): Model name
- `modality` (TEXT): Modality (Vision, Language, Speech, 3D, Multimodal, Protein, Vector)
- `downloads_last_month` (INTEGER): Download count for the month
- `likes` (INTEGER): Like count
- `discussions` (INTEGER): Discussion count
- `descendants` (INTEGER): Descendant model count
- `date_crawl` (TEXT): Crawl date

**`datasets` table**
- `id` (TEXT): Full identifier (primary key)
- `month` (TEXT): Month in YYYY-MM format (primary key)
- `org` (TEXT): Organization name
- `repo` (TEXT): Repository name
- `name` (TEXT): Dataset name
- `modality` (TEXT): Modality (Language, Speech, Vision, Multimodal, Embodied)
- `lifecycle` (TEXT): Lifecycle (Pre-training, Fine-tuning, Preference)
- `downloads_last_month` (INTEGER): Download count for the month
- `likes` (INTEGER): Like count
- `discussions` (INTEGER): Discussion count
- `descendants` (INTEGER): Descendant dataset count
- `date_crawl` (TEXT): Crawl date

### MCP Server

The project includes an MCP (Model Context Protocol) server that provides safe read-only access to the database. This is configured in `.mcp.json` and used by the `osir-lmts-analyst` skill.

Available MCP tools:
- `query_osir_lmts_db`: Execute read-only SQL queries
- `get_db_schema`: Get database schema information
- `get_available_months`: Get list of available months
- `get_latest_month`: Get the latest month

### Data Analysis Skill

The `osir-lmts-analyst` Claude Code skill provides interactive data analysis capabilities:
- Query download trends over time
- Analyze organization rankings
- Generate visualizations (line charts, pie charts, bar charts, radar charts, etc.)
- Compare monthly changes

#### Quick Demo

1. Launch Claude Code in the project directory
2. Run the analysis skill with your question:
   ```
   /osir-lmts-analyst 你需要询问/分析的问题
   ```

Example queries:
- `/osir-lmts-analyst 查看最新月份的模型下载量排名前10`
- `/osir-lmts-analyst 分析过去6个月语言模型的下载趋势`
- `/osir-lmts-analyst 对比上个月和本月机构排名的变化`

For the Open Source Influence Ranking of Large Model Technology Stack (OSIR-LMTS), see the [osir_lmts/README.md](osir_lmts/README.md) for detailed documentation in Chinese.

## License

TBD
