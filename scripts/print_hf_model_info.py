from pprint import pprint
from huggingface_hub import HfApi, ModelCard

api = HfApi()

models = api.list_models(author='deepseek-ai', model_name='DeepSeek-V3.2-Speciale')
model = next(models)  # type: ignore
pprint(model)

# ModelInfo(id='deepseek-ai/DeepSeek-V3.2-Speciale',
#           author=None,
#           sha=None,
#           created_at=datetime.datetime(2025, 11, 28, 3, 0, 2, tzinfo=datetime.timezone.utc),
#           last_modified=None,
#           private=False,
#           disabled=None,
#           downloads=26739,
#           downloads_all_time=None,
#           gated=None,
#           gguf=None,
#           inference=None,
#           inference_provider_mapping=None,
#           likes=640,
#           library_name='transformers',
#           tags=['transformers',
#                 'safetensors',
#                 'deepseek_v32',
#                 'text-generation',
#                 'base_model:deepseek-ai/DeepSeek-V3.2-Exp-Base',
#                 'base_model:finetune:deepseek-ai/DeepSeek-V3.2-Exp-Base',
#                 'license:mit',
#                 'endpoints_compatible',
#                 'fp8',
#                 'region:us'],
#           pipeline_tag='text-generation',
#           mask_token=None,
#           card_data=None,
#           widget_data=None,
#           model_index=None,
#           config=None,
#           transformers_info=None,
#           trending_score=10,
#           siblings=None,
#           spaces=None,
#           safetensors=None,
#           security_repo_status=None)

discussions = api.get_repo_discussions(model.id)
discussions = list(discussions)
pprint(f'Total discussions: {len(discussions)}')
pprint(discussions[0])

# Discussion(title='GGUF quants?',
#            status='open',
#            num=17,
#            repo_id='deepseek-ai/DeepSeek-V3.2-Speciale',
#            repo_type='model',
#            author='fsaudm',
#            is_pull_request=False,
#            created_at=datetime.datetime(2025, 12, 29, 15, 48, 12, tzinfo=datetime.timezone.utc),
#            endpoint='https://huggingface.co')

dis_details = api.get_discussion_details(model.id, 17)
pprint(len(dis_details.events))

# 3

model_card = ModelCard.load(model.id)
print(model_card)
