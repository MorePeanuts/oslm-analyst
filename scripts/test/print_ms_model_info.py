"""
This script is used to print the output format and content of some commonly used functions in the
modelscope.hub library.
"""

from modelscope.hub.info import ModelInfo, DatasetInfo

from pprint import pprint
from modelscope.hub.api import HubApi

api = HubApi()

# Get model list:
models = api.list_models(owner_or_group='deepseek-ai', page_number=1, page_size=10)
print(models.keys())
print('DeepSeek total models:', models['TotalCount'])

datasets = api.list_datasets(owner_or_group='moonshotai')
print(datasets.keys())
print('Moonshot total datasets:', datasets['total_count'])

# first model
model_info = ModelInfo(**models['Models'][0])
pprint(model_info)

# first dataset
dataset_info = DatasetInfo(**datasets['datasets'][0])
pprint(dataset_info)

# Retrieve specific model information by ID:
model_info = api.model_info('deepseek-ai/DeepSeek-V3.2-Speciale')
print('id:', model_info.id)
print('name:', model_info.name)
print('author:', model_info.author)
print('downloads:', model_info.downloads)
print('likes:', model_info.likes)
print('description:', model_info.description)
# print('readme:\n', model_info.readme_content)
print('tags:\n', model_info.tags)

# equal to:
model_info = api.repo_info('deepseek-ai/DeepSeek-V3.2-Speciale', repo_type='model')

# branches:
branches, tags = api.get_model_branches_and_tags('deepseek-ai/DeepSeek-V3.2-Speciale')
print('branches')
pprint(branches)
print('tags')
pprint(tags)

url = api.get_model_url('deepseek-ai/DeepSeek-V3.2-Speciale')
print(url)
