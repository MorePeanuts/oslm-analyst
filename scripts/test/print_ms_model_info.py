from pprint import pprint
from modelscope.hub.api import HubApi

api = HubApi()

models = api.list_models(owner_or_group='deepseek-ai', page_number=1, page_size=10)
print('DeepSeek total models:', models['TotalCount'])
model = api.get_model('deepseek-ai/DeepSeek-V3.2-Speciale')

print('model_id:', model['BackendSupport']['model_id'])
print('downloads:', model['Downloads'])
print('stars/likes:', model['Stars'])
print('model_name:', model['Name'])
print('repo:', model['Organization']['Name'])

readme = model['ReadMeContent']
print('storage:', model['StorageSize'], 'B')
print('model_size:', model['ModelInfos']['safetensor']['model_size'])

print('------------------\n')

model_info = api.model_info('deepseek-ai/DeepSeek-V3.2-Speciale')
print('id:', model_info.id)
print('name:', model_info.name)
print('author:', model_info.author)
print('downloads:', model_info.downloads)
print('likes:', model_info.likes)
print('siblings:', len(model_info.siblings))  # type: ignore
print('description:', model_info.description)
readme = model_info.readme_content
