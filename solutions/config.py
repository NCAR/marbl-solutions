import yaml

with open('deploy.yml', mode='r') as fptr:
    deploy_config = yaml.safe_load(fptr)
    
with open('analysis.yml', mode='r') as fptr:
    analysis_config = yaml.safe_load(fptr)
    
# Setup the epoch_mean_configuration
epoch_mean_config = analysis_config['epoch_mean']

variables = []
for component in list(epoch_mean_config['component'].keys()):
    variables+=list(epoch_mean_config['component'][component].keys())

epoch_mean_config['variables'] = variables
