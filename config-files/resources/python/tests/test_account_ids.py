import json
import os
from .. import account_ids
from unittest.mock import Mock, patch
TEST_DIRECTORY_PATH = "test_data/"


def load_file_from_path(file_name_with_ext, is_json_file=False, is_text_file=False):
    file_path = TEST_DIRECTORY_PATH + file_name_with_ext
    try:
        absolute_path = os.path.join(os.path.dirname(__file__), file_path)
        with open(absolute_path, 'r') as file:
            if is_json_file:
                data = json.load(file)
            elif is_text_file:
                data = file.read().strip()
            return data
    except FileNotFoundError as file_not_found_error:
        print(f"Error: File not found at {file_path}")
        return file_not_found_error
    except json.JSONDecodeError as decode_error:
        print(f"Error: File at {file_path} is not valid JSON")
        return decode_error
    except Exception as e:
        print(f"An error occurred: {e}")
        return e


def test_single_account_id_from_ou():
    ou_target_account = ['Workloads/Pro']
    ou_with_all_accounts = load_file_from_path(
        file_name_with_ext="ou_with_all_accounts.json",
        is_json_file=True
    )
    actual_output = account_ids.account_ids_from_ou(
        ou_target_accounts=ou_target_account,
        ou_with_all_accounts=ou_with_all_accounts
    )
    assert actual_output == {"TestProdWorkload": '777777777777'}


def test_multi_account_id_from_ou():
    ou_target_account = ['Workloads/Pro', 'Workloads/Dev']
    ou_with_all_accounts = load_file_from_path(
        file_name_with_ext="ou_with_all_accounts.json",
        is_json_file=True
    )
    actual_output = account_ids.account_ids_from_ou(
        ou_target_accounts=ou_target_account,
        ou_with_all_accounts=ou_with_all_accounts
    )
    assert actual_output == {"TestProdWorkload": '777777777777', 'TestWorkload': '666666666666'}


def test_single_account_id_from_name():
    deployment_target_accounts = ["Audit"]
    ou_with_all_accounts = load_file_from_path(
        file_name_with_ext="ou_with_all_accounts.json",
        is_json_file=True
    )
    actual_output = account_ids.account_ids_from_name(
        deployment_target_accounts=deployment_target_accounts,
        ou_org_accounts=ou_with_all_accounts
    )
    assert actual_output == {'Audit': '000000000000'}


def test_multi_account_id_from_name():
    deployment_target_accounts = ["Management", "Audit"]
    ou_with_all_accounts = load_file_from_path(
        file_name_with_ext="ou_with_all_accounts.json",
        is_json_file=True
    )
    actual_output = account_ids.account_ids_from_name(
        deployment_target_accounts=deployment_target_accounts,
        ou_org_accounts=ou_with_all_accounts
    )
    assert actual_output == {'Management': '123456789012', 'Audit': '000000000000'}


def test_create_provider_block_no_management():
    account_name_with_ids = {'Audit': 555555555555}
    regions = {'eu-west-2'}
    expected_output = load_file_from_path(
        file_name_with_ext="no_management_provider_block_expected_output.txt",
        is_text_file=True
    ) + "\n\n"
    actual_output = account_ids.create_provider_code(
        account_ids_by_name=account_name_with_ids,
        regions=regions
    )
    assert actual_output == expected_output


def test_create_provider_block_with_management():
    account_name_with_ids = {'Management': 123456789012, 'Audit': 555555555555}
    regions = {'eu-west-2'}
    expected_output = load_file_from_path(
        file_name_with_ext="management_provider_block_expected_output.txt",
        is_text_file=True
    ) + "\n\n"
    actual_output = account_ids.create_provider_code(
        account_ids_by_name=account_name_with_ids,
        regions=regions
    )
    assert actual_output == expected_output


def test_get_all_accounts():
    mock_org_client = Mock()
    mock_paginator = Mock()
    mock_api_response = load_file_from_path(
        file_name_with_ext="accounts_api_example_call.json",
        is_json_file=True
    )
    mock_paginator.paginate.return_value = iter([mock_api_response])
    mock_org_client.get_paginator.return_value = mock_paginator

    expected_output = {
        'Audit': '444444444444',
        'Backups': '222222222222',
        'LogArchive': '999999999999',
        'Management': '123456789012',
        'NetworkServices': '555555555555',
        'PolicyDevelopment': '888888888888',
        'SharedServices': '111111111111',
        'TestPreWorkload': '333333333333',
        'TestProdWorkload': '777777777777',
        'TestSandbox': '000000000000',
        'TestWorkload': '666666666666'
    }

    actual_output = account_ids.get_all_accounts(org_client=mock_org_client)

    assert actual_output == expected_output


def test_create_module():
    deployment_ids = {'Management': '123456789012', 'Audit': '000000000000'}
    expected_output = load_file_from_path(
        file_name_with_ext="create_module_code_expected_output.txt",
        is_text_file=True
    ) + "\n\n"

    actual_output = account_ids.create_module_code(
        module_name="my-dummy-module",
        module_source="github.com/cloudscaler/my-dummy-module",
        deployment_account_ids=deployment_ids,
        regions={"eu-west-2"},
        variables=[
            {
                'name': 'allowed_source_org_ous',
                'value': ['o-abc/def/ghi']
            },
            {
                'name': 'dummy_api_key',
                'value': 'myDummyAPIKey'
            },
            {
                'name': 'complex_var',
                'value': {
                    'test_var_string': "this is a string",
                    'test_var_int': 10,
                    'test_var_bool': True,
                    'test_var_list': ['o-abc/def/ghi'],
                    'test_var_dict': {
                        'dict_1': "this is a complex var"
                    }
                }
            },
        ],
        module_dependencies=[]
    )

    assert actual_output == expected_output
