from modelscope.hub.api import HubApi
from huggingface_hub import HfApi


# api = HubApi()
# info = api.model_info('BAAI/RoboBrain2.0-32B')
api = HfApi()
discussions = api.get_repo_discussions('nvidia/Nemotron-ClimbMix', repo_type='dataset')
total = 0
for dis in discussions:
    details = api.get_discussion_details('nvidia/Nemotron-ClimbMix', dis.num, repo_type='dataset')
    print(len(details.events))
    total += 1

print(total)
