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

### In Progress / Planned
- **Data Analysis Agent**: AI-powered agent for answering questions about the data
- **Database Setup**: Persistent storage for historical data and efficient querying

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

# Launch dashboard
uv run oslm-analyst dashboard
```

## Project Structure

```
OSLM-Analyst/
├── config/              # Configuration files
├── output/              # Output data directory
├── osir_lmts/           # OSIR-LMTS specific files
├── scripts/             # Utility scripts
├── src/oslm_analyst/    # Main source code
│   ├── crawl.py         # Crawl orchestrator
│   ├── crawlers/        # Platform-specific crawlers
│   ├── processors/      # Data processors
│   ├── ui/              # Streamlit dashboard
│   └── cli.py           # CLI entrypoint
└── tests/               # Tests
```

## OSIR-LMTS

For the Open Source Influence Ranking of Large Model Technology Stack (OSIR-LMTS), see the [osir_lmts/README.md](osir_lmts/README.md) for detailed documentation in Chinese.

## License

TBD