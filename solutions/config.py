import yaml

with open('deploy.yml', mode='r') as fptr:
    deploy_config = yaml.safe_load(fptr)
