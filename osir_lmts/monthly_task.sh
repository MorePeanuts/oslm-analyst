uv run oslm-analyst crawl huggingface --category model
uv run oslm-analyst crawl huggingface --category dataset
uv run oslm-analyst crawl modelscope --category model
uv run oslm-analyst crawl modelscope --category dataset
uv run oslm-analyst crawl baai-datahub

uv run oslm-analyst process gen-modality output/huggingface_2026-01-01
uv run oslm-analyst process gen-modality output/modelscope_2026-01-01
uv run oslm-analyst process gen-modality output/baai-datahub_2026-01-01

uv run oslm-analyst process osir-lmts
