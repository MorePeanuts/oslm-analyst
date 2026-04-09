---
name: osir-lmts-analyst
description: OSIR-LMTS 数据分析助手。使用 MCP 工具安全地查询 osir_lmts.db 数据库，分析模型/数据集排名趋势。当用户询问关于 OSIR-LMTS 项目的数据问题、需要查询下载量趋势、机构排名、月度变化等问题时使用此技能。**注意：仅当用户明确要求生成图表时才生成图表，仅当查询无法满足需求时才编写分析代码。**
---

# OSIR-LMTS 数据分析助手

此技能用于帮助分析 OSIR-LMTS（Open Source AI Resource - Large Model Tracking System）项目的数据。

## 核心原则

**重要！请务必遵循：**

1. **默认只查询数据，直接回答** - 对于大多数问题，只需使用 MCP 工具查询数据，然后用自然语言总结结果即可
2. **仅在用户明确要求时才生成图表** - 不要主动生成图表，除非用户说"画一个图"、"可视化"、"生成图表"等
3. **仅在必要时才编写分析代码** - 只有当问题过于复杂，无法仅通过 SQL 查询完成时，才编写 Python 代码进行深度分析
4. **所有输出文件放在 analysis 文件夹** - 任何生成的代码、图表、表格都必须放在 `analysis/{timestamp}/` 目录下，其中 `{timestamp}` 是当前时间戳（格式：YYYYMMDD_HHMMSS）

## 创建工作目录

**在需要生成文件或图表时，首先使用 Bash 命令创建工作目录：**

```bash
# 创建带时间戳的工作目录
timestamp=$(date +"%Y%m%d_%H%M%S")
mkdir -p "analysis/${timestamp}"
echo "analysis/${timestamp}"
```

执行上述命令后，将输出的目录路径保存，后续所有生成的文件都写入该目录。

## 重要：使用 MCP 工具查询数据库

**不要直接编写 Python 代码连接数据库！** 使用以下 MCP 工具安全地查询数据库：

1. `query_osir_lmts_db` - 执行只读 SQL 查询
2. `get_db_schema` - 获取数据库结构
3. `get_available_months` - 获取可用月份列表
4. `get_latest_month` - 获取最新月份

## 数据库结构

数据库包含两个主要表：

### 跟踪机构列表

数据库跟踪以下机构的模型和数据集：

| 机构名称 | 说明 |
|---------|------|
| BAAI | 北京智源人工智能研究院 |
| ShanghaiAILab | 上海人工智能实验室 |
| Baidu | 百度 |
| Baichuan | 百川智能 |
| Zhipu | 智谱 AI |
| Ali | 阿里巴巴 |
| Huawei | 华为 |
| LMSYS | LMSYS 组织 |
| Falcon | Falcon |
| EleutherAI | EleutherAI |
| Meta | Meta |
| Google | 谷歌 |
| Deepseek | 深度求索 |
| OpenBMB | OpenBMB |
| 01-AI | 零一万物 |
| Ai2 | Allen Institute for AI |
| OpenAI | OpenAI |
| Nvidia | 英伟达 |
| Microsoft | 微软 |
| Apple | 苹果 |
| HuggingFace | HuggingFace |
| Princeton NLP | 普林斯顿大学 NLP 实验室 |
| UKP Lab | UKP 实验室 |
| Tencent | 腾讯 |
| ByteDance | 字节跳动 |

### `models` 表
| 列名 | 类型 | 说明 |
|------|------|------|
| id | TEXT | 主键1 - 完整标识符（org/model-name） |
| month | TEXT | 主键2 - 月份（YYYY-MM格式） |
| org | TEXT | 机构名称 |
| repo | TEXT | 仓库名称 |
| name | TEXT | 模型名称 |
| modality | TEXT | 模态（Vision, Language, Speech, 3D, Multimodal, Protein, Vector） |
| downloads_last_month | INTEGER | 当月下载量 |
| likes | INTEGER | 点赞数 |
| discussions | INTEGER | 讨论数 |
| descendants | INTEGER | 派生模型数 |
| date_crawl | TEXT | 爬取日期 |

### `datasets` 表
| 列名 | 类型 | 说明 |
|------|------|------|
| id | TEXT | 主键1 - 完整标识符 |
| month | TEXT | 主键2 - 月份（YYYY-MM格式） |
| org | TEXT | 机构名称 |
| repo | TEXT | 仓库名称 |
| name | TEXT | 数据集名称 |
| modality | TEXT | 模态（Language, Speech, Vision, Multimodal, Embodied） |
| lifecycle | TEXT | 生命周期（Pre-training, Fine-tuning, Preference） |
| downloads_last_month | INTEGER | 当月下载量 |
| likes | INTEGER | 点赞数 |
| discussions | INTEGER | 讨论数 |
| descendants | INTEGER | 派生数据集数 |
| date_crawl | TEXT | 爬取日期 |

## 可用模态类型

**模型模态：**
- Vision（视觉）
- Language（语言）
- Speech（语音）
- 3D
- Multimodal（多模态）
- Protein（蛋白质）
- Vector（向量）

**数据集模态：**
- Language（语言）
- Speech（语音）
- Vision（视觉）
- Multimodal（多模态）
- Embodied（具身）

**数据集生命周期：**
- Pre-training（预训练）
- Fine-tuning（微调）
- Preference（偏好）

## 工作流程

### 标准流程（大多数情况使用这个）

1. **理解用户问题** - 确定用户需要查询什么数据（模型/数据集、时间范围、筛选条件）
2. **获取元数据（如果需要）** - 使用 `get_latest_month` 或 `get_available_months` 获取时间范围
3. **构造 SQL 查询** - 使用 `query_osir_lmts_db` 工具执行查询
4. **直接回答用户** - 用自然语言总结查询结果，展示关键数据

### 仅在用户要求时才生成图表

当用户明确要求可视化、画图、生成图表时：

1. 遵循标准流程的 1-3 步
2. **编写 Python 代码** - 在工作目录下创建 `.py` 文件，使用 matplotlib/pandas 绘图
3. **执行代码生成图表** - 运行代码，将图表保存为图片文件（PNG 格式）
4. **解释结果** - 用自然语言总结分析结果，说明图表展示的内容

### 仅在必要时才编写深度分析代码

只有当问题过于复杂，无法通过 SQL 查询直接回答时（例如：复杂的统计分析、预测、自定义算法等）：

1. 先尝试用 SQL 查询获取尽可能多的数据
2. **编写 Python 代码** - 在工作目录下创建 `.py` 文件进行深度分析
3. **执行代码** - 运行代码并保存结果
4. **解释结果** - 用自然语言总结分析结果

## 常用查询示例

### 1. 查询某月数据
使用 `query_osir_lmts_db` 工具：
```sql
-- 查询某月所有模型
SELECT * FROM models WHERE month = '2026-03'

-- 查询某月语言模型
SELECT * FROM models 
WHERE month = '2026-03' AND modality = 'Language'
```

### 2. 按机构统计
```sql
-- 某月某机构下载量总和
SELECT org, SUM(downloads_last_month) as total_downloads
FROM models 
WHERE month = '2026-03'
GROUP BY org
ORDER BY total_downloads DESC
```

### 3. 趋势查询
```sql
-- 某模型的月度下载量趋势
SELECT month, downloads_last_month
FROM models 
WHERE id = 'org/model-name'
ORDER BY month
```

### 4. 环比变化
```sql
-- 比较连续两个月的数据
SELECT 
    m1.org, m1.name,
    m1.downloads_last_month as current_month,
    m2.downloads_last_month as previous_month,
    m1.downloads_last_month - m2.downloads_last_month as delta
FROM models m1
LEFT JOIN models m2 
    ON m1.id = m2.id 
    AND m2.month = '2026-02'
WHERE m1.month = '2026-03'
```

## 回答示例

### 示例 1：简单查询（直接回答，不生成图表）

**用户问题：** 最新一个月哪一个机构的语言模型下载量最高？

**步骤 1：** 使用 `get_latest_month` 获取最新月份

**步骤 2：** 使用 `query_osir_lmts_db` 查询数据：
```sql
SELECT org, SUM(downloads_last_month) as total_downloads,
       COUNT(*) as model_count
FROM models 
WHERE month = '{latest_month}' AND modality = 'Language'
GROUP BY org
ORDER BY total_downloads DESC
LIMIT 10
```

**步骤 3：** 直接回答：
```
根据 {latest_month} 的数据，语言模型下载量最高的机构是：

1. {第一名机构} - 下载量 {下载量:,}，模型数 {模型数}
2. {第二名机构} - 下载量 {下载量:,}，模型数 {模型数}
3. {第三名机构} - 下载量 {下载量:,}，模型数 {模型数}
...
```

### 示例 2：用户要求生成图表

**用户问题：** 分析 2025 年语言模型下载量月度变化趋势，画一个折线图。

**步骤 1：** 使用 `query_osir_lmts_db` 查询数据：
```sql
SELECT month, SUM(downloads_last_month) as total_downloads
FROM models 
WHERE modality = 'Language' 
  AND month LIKE '2025-%'
GROUP BY month
ORDER BY month
```

**步骤 2：** 在工作目录下编写 Python 脚本 `plot_trend.py`：
```python
import pandas as pd
import matplotlib.pyplot as plt
import json
from pathlib import Path

# 假设 query_result 是 MCP 工具返回的数据
data = query_result  # 实际使用时替换为真实数据
df = pd.DataFrame(data['rows'])

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'SimHei']
plt.rcParams['axes.unicode_minus'] = False

# 创建输出目录
output_dir = Path(__file__).parent
print("2025年语言模型月度下载量：")
print(df)

# 绘制折线图
plt.figure(figsize=(12, 6))
plt.plot(df['month'], df['total_downloads'], marker='o', linewidth=2, color='#2e86ab')
plt.fill_between(df['month'], df['total_downloads'], alpha=0.3, color='#2e86ab')
plt.title('2025年语言模型下载量月度变化趋势', fontsize=14)
plt.xlabel('月份', fontsize=12)
plt.ylabel('总下载量', fontsize=12)
plt.xticks(rotation=45)
plt.grid(True, alpha=0.3)
plt.tight_layout()

# 保存图表
chart_path = output_dir / 'language_model_trend.png'
plt.savefig(chart_path, dpi=300, bbox_inches='tight')
print(f"\n图表已保存至: {chart_path}")
plt.show()
```

**步骤 4：** 执行代码并回答用户，说明结果和图表位置。
```bash
uv run path/to/workspace/plot_trend.py
```

## 绘图工具（仅在需要时使用）

使用 matplotlib 和 pandas 进行数据可视化。所有代码和图表保存在 `analysis/{timestamp}/` 目录下。

### 折线图（趋势图）
```python
plt.figure(figsize=(12, 6))
plt.plot(df['month'], df['total_downloads'], marker='o', linewidth=2)
plt.title('标题', fontsize=14)
plt.xlabel('月份', fontsize=12)
plt.ylabel('总下载量', fontsize=12)
plt.xticks(rotation=45)
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(output_dir / 'chart.png', dpi=300, bbox_inches='tight')
plt.show()
```

### 饼图（比例图）
```python
plt.figure(figsize=(10, 10))
plt.pie(df['total_downloads'], labels=df['modality'], autopct='%1.1f%%')
plt.title('标题', fontsize=14)
plt.savefig(output_dir / 'pie_chart.png', dpi=300, bbox_inches='tight')
plt.show()
```

### 柱状图（对比图）
```python
plt.figure(figsize=(12, 6))
plt.bar(df['org'], df['total_downloads'])
plt.title('标题', fontsize=14)
plt.xlabel('机构', fontsize=12)
plt.ylabel('总下载量', fontsize=12)
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(output_dir / 'bar_chart.png', dpi=300, bbox_inches='tight')
plt.show()
```

### 雷达图
```python
import numpy as np

modalities = ['Vision', 'Language', 'Speech', '3D', 'Multimodal', 'Vector']
values = [100, 500, 50, 20, 200, 150]

angles = np.linspace(0, 2 * np.pi, len(modalities), endpoint=False)
values = np.concatenate((values, [values[0]]))
angles = np.concatenate((angles, [angles[0]]))

fig, ax = plt.subplots(figsize=(8, 8), subplot_kw=dict(projection='polar'))
ax.plot(angles, values, 'o-', linewidth=2)
ax.fill(angles, values, alpha=0.25)
ax.set_xticks(angles[:-1])
ax.set_xticklabels(modalities)
ax.set_title('标题', fontsize=14, y=1.1)
plt.savefig(output_dir / 'radar_chart.png', dpi=300, bbox_inches='tight')
plt.show()
```

### 堆叠面积图
```python
df_pivot = df.pivot(index='month', columns='org', values='downloads').fillna(0)

plt.figure(figsize=(14, 7))
df_pivot.plot(kind='area', stacked=True, ax=plt.gca())
plt.title('标题', fontsize=14)
plt.xlabel('月份', fontsize=12)
plt.ylabel('总下载量', fontsize=12)
plt.xticks(rotation=45)
plt.legend(title='机构', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.savefig(output_dir / 'area_chart.png', dpi=300, bbox_inches='tight')
plt.show()
```

## 注意事项

- 始终使用 MCP 工具查询数据库，**不要**直接使用 sqlite3 连接
- `downloads_last_month` 字段表示该月的下载量（非累积）
- 月份格式为 `YYYY-MM`（如 `2026-03`）
- 绘图时注意中文字体设置，避免乱码
- **记住：默认只查询数据直接回答，图表和分析代码仅在必要时生成！**
- 所有输出文件必须放在 `analysis/{timestamp}/` 目录下，不要修改项目其他部分
- 使用 `uv run xxx.py` 运行python脚本
