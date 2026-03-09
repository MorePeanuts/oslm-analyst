from modelscope.hub.api import HubApi


api = HubApi()
info = api.model_info('BAAI/RoboBrain2.0-32B')
