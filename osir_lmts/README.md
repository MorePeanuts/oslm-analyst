# OSIR-LMTS

大模型技术栈开源影响力排名 (Open Source Influence Ranking of Large Model Technology Stack)

## 项目概述

OSIR-LMTS (Open Source Influence Ranking of Large Model Technology Stack) 致力于构建一个系统化的大模型技术体系开源影响力评估框架。我们通过科学、可操作的评估标准，旨在帮助开发者、研究者和企业更准确地理解和衡量开源大模型技术体系的实际价值。

我们的目标是从**数据、模型、系统和评测平台**四个关键技术维度，全面评估开源大模型技术体系的影响力。

## 评估维度

### 数据维度
- 数据集覆盖度
- 大模型生命周期覆盖度
- 数据处理工具

### 模型维度
- 模型的使用量
- 模型模态覆盖度
- 模型规模
- 贡献者活跃度
- 模型开源开放度
- 模型适配的芯片数量

### 系统维度
- 算子库
- 并行训练与推理框架
- 深度学习框架
- 开源 AI 编译器
- 通信库
- 贡献者活跃度

### 评测平台维度
- 评测榜单
- 评测模型
- 评测数据
- 评测方法

这种对大模型技术体系维度的全面覆盖，保证了评估框架在衡量开源大模型技术体系时的系统性和科学性。所有指标均采用 **Min-Max 正则化后求平均** 的方式计算影响力得分。

## 使用方法

完整的数据处理流程分为三个步骤，可参考 [monthly_task.sh](monthly_task.sh) 脚本。

### 第一步：获取数据

从各平台爬取模型和数据集数据：

```bash
# 从 HuggingFace 爬取模型和数据集
uv run oslm-analyst crawl huggingface --category model
uv run oslm-analyst crawl huggingface --category dataset

# 从 ModelScope 爬取模型和数据集
uv run oslm-analyst crawl modelscope --category model
uv run oslm-analyst crawl modelscope --category dataset

# 从 BAAI DataHub 爬取数据
uv run oslm-analyst crawl baai-datahub
```

### 第二步：生成/更新模态信息

使用 AI 辅助生成模态和生命周期信息：

```bash
# 为 HuggingFace 数据生成模态信息
uv run oslm-analyst process gen-modality output/huggingface_YYYY-MM-DD

# 为 ModelScope 数据生成模态信息
uv run oslm-analyst process gen-modality output/modelscope_YYYY-MM-DD

# 为 BAAI DataHub 数据生成模态信息
uv run oslm-analyst process gen-modality output/baai-datahub_YYYY-MM-DD
```

### 第三步：计算排名

处理数据并生成排名：

```bash
# 处理当前月份数据（自动使用上月）
uv run oslm-analyst process osir-lmts

# 指定月份处理
uv run oslm-analyst process osir-lmts 2026-03
```

### 第四步：查看数据

启动 Dashboard 查看可视化结果：

```bash
uv run oslm-analyst dashboard
```

## 输出文件

处理完成后，会在 `output/osir-lmts_YYYY-MM/` 目录下生成以下文件：

### 数据文件
- `model_data.jsonl` / `dataset_data.jsonl` - 月度增量数据
- `acc_model_data.jsonl` / `acc_dataset_data.jsonl` - 累积数据

### 摘要文件
- `model_summary.csv` / `dataset_summary.csv` - 当月摘要
- `acc_model_summary.csv` / `acc_dataset_summary.csv` - 累积摘要
- `delta_model_summary.csv` / `delta_dataset_summary.csv` - 变化量摘要
- `infra_summary.csv` / `eval_summary.csv` - 基础设施和评测摘要

### 排名文件
- `model_rank.csv` / `dataset_rank.csv` / `infra_rank.csv` / `eval_rank.csv` - 各维度排名
- `overall_rank.csv` - 总排名
- `acc_*.csv` - 累积排名
- `CN_*.csv` - 国内机构排名

## 配置文件

- `config/orgs.yaml` - 要跟踪的机构列表
- `config/model_info.jsonl` - 模型的额外信息（模态、有效性）
- `config/dataset_info.jsonl` - 数据集的额外信息（模态、生命周期、有效性）
- `config/osir_lmts_orgs.json` - 参与排名的机构列表

## 排名策略

详细策略请参考 `src/oslm_analyst/processors/osir_lmts_rank.py`。
