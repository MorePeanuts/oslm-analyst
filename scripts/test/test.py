from modelscope.hub.api import HubApi
from huggingface_hub import HfApi


# api = HubApi()
# info = api.model_info('BAAI/RoboBrain2.0-32B')
api = HfApi()
info = api.get_user_overview('alibabasglab')
print(info)
