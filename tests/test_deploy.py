import solutions


def test_deploy_config():
    deploy_config = solutions.config.deploy_config
    assert deploy_config['reference_case'] == 'ref_case'
    assert type(deploy_config['reference_case_path']) == list
    assert deploy_config['reference_case_file_format'] == 'history'
    assert deploy_config['case_to_compare_file_format'] == 'timeseries'
