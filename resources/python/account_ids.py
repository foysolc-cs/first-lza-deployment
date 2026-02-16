import copy
import logging
import time
import os
from typing import Dict

import boto3
from botocore.config import Config
import yaml as yml
from mypy_boto3_organizations import OrganizationsClient

YAML_FILE_NAME = "customizations-terraform.yaml"
DEBUG_YAML_FILE_NAME = "debugging_files/test-customizations-terraform.yaml"
NAME_OF_OU_ROOT = "root"
ALL_REGIONS = set()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s - \"%(funcName)s\" - %(message)s")


def load_configuration(yaml_file_name: str) -> dict:
    """
    Loads the yaml file containing the config for the tf module deployment

    :param yaml_file_name: File name it could contain a path for the file, if in debug mode
    :return: Returns the yaml as a dict
    """
    logger.info(f"Attempting to load '{yaml_file_name}'...")
    with open(f'{yaml_file_name}', 'r', encoding='us-ascii') as file:
        docs = yml.safe_load_all(file)
        for doc in docs:
            yaml = doc
    logger.info(f"'{yaml_file_name}' loaded.")
    logger.debug(f"Yaml returned: {yaml}")
    return yaml


def get_root_id(org_client: OrganizationsClient) -> str:
    """
    Gets root id for Org

    Args:
        org_client: Object representing the aws organizations client.

    Returns:
        string containing root id of Org
    """
    root_data = org_client.list_roots()
    root_id = root_data.get("Roots")[0].get("Id")
    logger.info(f"Root ID org call successful, value recieved '{root_id}'")
    return root_id


def get_all_accounts(org_client: OrganizationsClient) -> dict:
    """
    Gets all account IDs in the organization.
    """
    accounts = {}
    paginator = org_client.get_paginator('list_accounts')
    for page in paginator.paginate():
        for account in page['Accounts']:
            accounts[account.get("Name")] = account.get("Id")
    return accounts


def get_all_regions() -> list:
    """
    Gets all AWS regions. You may want to customize this based on your needs.
    """
    ec2_client = boto3.client('ec2')
    regions = [region['RegionName'] for region in ec2_client.describe_regions()['Regions']]
    logger.debug("Retrieved all regions: %s", regions)
    return regions


def get_all_accounts_recursive(org_client: OrganizationsClient, parent_id: str, name: str,
                               accounts_by_ou: dict) -> dict:
    """
    Recursive function that builds out a dict containing an entire org's accounts by their parent OU.

    Args:
        org_client: Object representing the aws organizations client.
        parent_id: ID of parent OU to the child OU
        name: OU by its name ("Workloads/Dev")
        accounts_by_ou: the dict containing accounts by OU that is progressively built via recursion. E.g:
        {'root': [{'Id': '123', 'Arn': 'arn:...', '...': '...'}, {...}], 'SharedServices': [{...}, {...}], '...': [{'...'}]}

    Returns:
        Returns the accounts_by_ou
    """
    accounts_paginator = org_client.get_paginator('list_accounts_for_parent')
    ou_paginator = org_client.get_paginator('list_organizational_units_for_parent')
    accounts = []

    for page in accounts_paginator.paginate(ParentId=parent_id):
        accounts += page['Accounts']

    accounts_by_ou[name] = accounts
    for page in ou_paginator.paginate(ParentId=parent_id):
        for ou in page['OrganizationalUnits']:
            if name == NAME_OF_OU_ROOT:
                ou_name = ou['Name']
            elif org_client.list_children(ParentId=parent_id, ChildType='ORGANIZATIONAL_UNIT')["Children"]:
                ou_name = f"{name}/{ou['Name']}"
            get_all_accounts_recursive(
                org_client=org_client,
                parent_id=ou['Id'],
                name=ou_name,
                accounts_by_ou=accounts_by_ou
            )

    return accounts_by_ou


def filter_excluded_accounts(org_account_list: dict, excluded_accounts: list) -> dict:
    """
    Rebuilds the org_account_list dict with "excluded_accounts" no longer present. As this dict is what is used to find
    accounts, it is an effective way to ignore excluded accounts.

    Args:
        org_account_list: dictionary of accounts (with their details) by OU e.g. :
        {'root': [{'Id': '123', 'Arn': 'arn:...', '...': '...'}, {...}], 'SharedServices': [{...}, {...}], '...': [{'...'}]}
        excluded_accounts: List of accounts to be excluded by their name e.g. ['TestWorkload', 'Workloads/Pre']

    Returns:
        Returns rebuilt param "org_account_list" with excluded accounts no longer present.
    """
    for ou, account in org_account_list.items():
        org_account_list[ou] = [
            acc for acc in account if not (
                    ou in excluded_accounts or acc.get("Name") in excluded_accounts
            )
        ]
    return org_account_list


def account_ids_from_ou(ou_target_accounts: list, ou_with_all_accounts: dict) -> Dict[str, int]:
    """
    Gets all accounts from ou_with_all_accounts based on the OU names in ou_target_accounts.

    Args:
        ou_target_accounts: list of OU names e.g. ['SharedServices', 'Workloads/Dev']
        ou_with_all_accounts: dictionary of accounts (with their details) by OU e.g.:
        {'root': [{'Id': '123', 'Arn': 'arn:...', '...': '...'}, {...}],
        'SharedServices': [{...}, {...}], '...': [{'...'}]}

    Returns:
        Dict in the format [str, int] of the found account IDs that need to be included as part of deployment
    """
    accounts = {}
    for ou in ou_target_accounts:
        for account_data in ou_with_all_accounts.get(ou):
            accounts[account_data.get("Name")] = account_data.get("Id")
    return accounts


def account_ids_from_name(deployment_target_accounts: list, ou_org_accounts: dict) -> Dict[str, int]:
    """
    Converts the name of an account to its account ID. This is achieved by finding the name in the "ou_org_accounts"
    param.

    Args:
        deployment_target_accounts: list of account names by word e.g. ['Management', 'TestWorkload']
        ou_org_accounts: dictionary of accounts (with their details) by OU e.g. :
        {'root': [{'Id': '123', 'Arn': 'arn:...', '...': '...'}, {...}], 'SharedServices': [{...}, {...}], '...': [{'...'}]}

    Returns:
        Dict in the format [str, int] of the found account IDs that need to be included as part of deployment
    """
    deployment_accounts = {}
    for ou in ou_org_accounts:
        for account in ou_org_accounts[ou]:
            account_name = account.get("Name")
            if account_name in deployment_target_accounts:
                deployment_target_accounts.remove(account_name)
                deployment_accounts[account_name] = account.get("Id")
    return deployment_accounts


def create_backend_code():
    """
    Generates empty Terraform backend block for modules to use
    """
    logger.info("Attempting to generate empty S3 backend configuration")

    backend_code = "terraform {\n"
    backend_code += "  backend \"s3\" {\n"
    backend_code += "    use_lockfile = true\n"
    backend_code += "  }\n"
    backend_code += "  required_version = \">= 1.11.1\"\n"
    backend_code += "}"

    write_to_file(
        content=backend_code,
        output_file="terraform/backend.tf"
    )


def create_provider_code(regions: list, account_ids_by_name: Dict[str, int]) -> str:
    """
    Generates Terraform provider blocks for modules to use

    Args:
        :param regions: List of regions
        :param account_ids_by_name: Dictionary of account ids by name
    """
    logger.info("Attempting to generate terraform file containing providers")
    # Management account id excluded below as no provider block for this account is required. This is as the account
    # the terraform pipeline is run in, is the management account.

    # Initialize empty string to store code
    provider_code_string_builder = ""
    for region in regions:
        for account_name, account_id in account_ids_by_name.items():
            if account_name == "Management":
                provider_code_string_builder += "provider \"aws\" {\n"
                provider_code_string_builder += f"  region = \"{region}\"\n"
                provider_code_string_builder += "}\n"
                provider_code_string_builder += "\n"
                continue
            account_name = account_name.lower()
            # Add provider declaration
            provider_code_string_builder += "provider \"aws\" {\n"
            provider_code_string_builder += "  assume_role {\n"
            # Role that can be assumed by the Management account
            provider_code_string_builder += f"    role_arn = \"arn:aws:iam::{account_id}:role/OrganizationAccountAccessRole\"\n"
            provider_code_string_builder += "  }\n"
            provider_code_string_builder += f"  alias  = \"{region}-{account_name}\"\n"
            provider_code_string_builder += f"  region = \"{region}\"\n"
            provider_code_string_builder += "}\n"
            provider_code_string_builder += "\n"

    write_to_file(
        content=provider_code_string_builder,
        output_file="terraform/provider.tf"
    )
    return provider_code_string_builder


def create_empty_main_tf() -> None:
    """
    This function will create an empty main.tf file when the test-customizations-terraform.yaml file has no modules defined. This is so that Terraform will be able to run
    either when no initial module deployments are required, or a requirement is in place to remove all modules from an LZA deployment.
    """
    write_to_file("", "terraform/main.tf")
    logger.warning("Empty main.tf file created for Terraform destory or zero module deployment operation.")


def create_module_code(module_name: str, module_source: str, deployment_account_ids: dict, regions: set,
                       variables: list[dict], module_dependencies: list) -> str:
    """
    Generates Terraform module code with for_each loop for deployment accounts.

    Args:
        module_name: The name of the Terraform module.
        module_source: The source of the Terraform module.
        deployment_account_ids: dict containing ids by account name, e.g. "{'Audit': '123...', 'ABC': '...'}".
        regions: List of region.
        variables: Map of variables for the module deployment
        module_dependencies: List of dependencies for the module.

    Returns:
        A string containing the generated Terraform module code.
    """
    logger.info(f"Attempting to generate terraform file for {module_name}")

    # Initialize empty string to store code
    module_code = ""

    for region in regions:
        for account_name, account_id in deployment_account_ids.items():
            account_name = account_name.lower()
            # Add module declaration
            module_code += f"module \"{module_name}-{region}-{account_name}\" {{\n"
            module_code += f"  source = \"{module_source}\"\n"
            # Handle variables for the module
            if variables:
                for var in variables:
                    name = var.get("name")
                    value = var.get("value")
                    formatted_value = format_value(value)
                    module_code += f"  {name} = {formatted_value}\n"
            else:
                logger.info(f"No variables specified for the '{module_name}'.")
            # Add provider configuration
            if account_name != "management":
                module_code += f"  providers = {{\n    aws = aws.{region}-{account_name}\n    }}\n"

            # Add depends_on configuration
            dependencies = module_dependencies
            if dependencies:
                module_code += "  depends_on = [ "
                for dependent in dependencies:
                    module_code += f"module.{dependent}-{region}, "
                module_code = module_code[:-2] + " ]\n"

            module_code += "}\n"
            module_code += "\n"
    write_to_file(
        content=module_code,
        output_file=f"terraform/{module_name}.tf"
    )
    return module_code


def format_value(value, indent=0):
    """
    Recursively formats the value based on its type (string, bool, int, list, or dict).

    Args:
        value: The value to be formatted.
        indent: Number of tabs to include to format the terraform
    Returns:
        The values formatted in the correct way for terraform.
    """

    indent_space = '  ' * indent

    if isinstance(value, str):
        if value.startswith("module."):
            return value  # No quotes for module references
        return f"\"{value}\""  # String with quotes if conversion fails

    elif isinstance(value, bool):
        return str(value).lower()

    elif isinstance(value, int):
        return value

    elif isinstance(value, list):
        if all(not isinstance(v, (dict, list)) for v in value):
            # Flat list
            indent += 1
            indent_space = '  ' * indent
            inner = ", ".join(format_value(v, indent) for v in value)
            return f"[{inner}]"
        else:
            # Complex list, format with indentation
            indent += 1
            indent_space = '  ' * indent
            inner = ",\n".join(f"{indent_space}  {format_value(v, indent)}" for v in value)
            return f"[\n{inner}\n{indent_space}]"

    elif isinstance(value, dict):
        indent += 1
        indent_space = '  ' * indent
        inner = "\n".join(f"{indent_space}  {k} = {format_value(v, indent)}" for k, v in value.items())
        return f"{{\n{inner}\n{indent_space}}}"

    else:
        raise TypeError(f"Unsupported type: {type(value)}, Value: {value}")
    return value


def write_to_file(content, output_file):
    """
    Writes content to a specified file.

    Args:
        content: The content to be written to the file (string).
        output_file: The path to the output file.
    """
    # Get the directory path from the output_file
    directory = os.path.dirname(output_file)

    # Check if directory exists, create it if not (using os.makedirs for recursive creation)
    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(output_file, "w", encoding='us-ascii') as file:
        file.write(content)
    logger.info(f"Content written to: {output_file}")

    return


def deploy_targets(org_client: OrganizationsClient, tf_modules: dict, root_id: str):
    """
    This function is the backbone of the script. It creates the dictionary containing all accounts by Org Unit of the
    organisation. This is then used to index account names with IDs and their OUs within subsequent functions. This
    helps by avoiding additional API overhead when calling the aws organizations endpoint.

    Once the IDs of the accounts have been found, the pipeline endpoint will be called to trigger the pipeline.

    Args:
        root_id: The root Id of the org, in the format similar to "r-abcd"
        org_client: Object representing the aws organizations client.
        tf_modules: a dict that contains all deployment targets containing: deployment target meta data (e.g. desc,
        name), tf module locations, and account names.
        shared_accounts_found: a set that will be populated with accounts found, so that we can generate tf providers
        at a later stage without having to be worried about duplicate entries

    Returns:
        None
    """
    account_ids_by_name = {}
    logger.info("Attempting to build dict containing all AWS accounts by OU recursively...")
    start_timer = time.time()
    ou_with_all_accounts = get_all_accounts_recursive(
        org_client=org_client,
        parent_id=root_id,
        name=NAME_OF_OU_ROOT,
        accounts_by_ou={}
    )
    end_timer = time.time()
    logger.info(f"Build complete. The build took {end_timer - start_timer} seconds to complete.")
    for key, value in ou_with_all_accounts.items():
        if isinstance(value, list):
            for account in value:
                account_ids_by_name[account.get("Name")] = account.get("Id")

    logger.info("Attempting to build list of all modules being deployed")
    all_modules = []
    for module in tf_modules:
        all_modules.append(module.get('name'))

    for tf_module in tf_modules:
        deployment_target_accounts = {}
        deployment_target = tf_module.get("deploymentTargets")
        module_name = tf_module.get("name")
        module_source = tf_module.get("source")
        regions = tf_module.get("regions")
        for region in regions:
            ALL_REGIONS.add(region)
        variable_inputs = tf_module.get("variables")
        dependencies = tf_module.get("dependsOn")
        all_org_accounts_copy = copy.deepcopy(ou_with_all_accounts)
        logger.info(f"Building pipeline deployment for module: '{module_name}'")

        if "excludedAccounts" in deployment_target:
            logger.info("Excluding accounts...")
            excluded_accounts = deployment_target.get("excludedAccounts", [])
            if excluded_accounts:
                try:
                    all_org_accounts_copy = filter_excluded_accounts(
                        org_account_list=ou_with_all_accounts,
                        excluded_accounts=excluded_accounts
                    )
                except Exception as e:
                    return logger.exception(f"Failed to filter excluded accounts due to : '{e}'")
            else:
                logger.warning(
                    f"'excludedAccounts' defined in yaml for the tf module '{module_name}' without any accounts, "
                    f"consider removing if not required."
                )
            logger.info("Excluding accounts step complete.")

        for target in deployment_target:
            match target:
                case "organizationalUnits":
                    ou_target_accounts = deployment_target.get("organizationalUnits")
                    if ou_target_accounts:
                        try:
                            deployment_target_accounts.update(
                                account_ids_from_ou(
                                    ou_target_accounts=ou_target_accounts,
                                    ou_with_all_accounts=all_org_accounts_copy
                                )
                            )
                        except Exception as e:
                            return logger.exception(f"Failed to filter excluded accounts due to : '{e}'")
                    else:
                        logger.warning(
                            f"'organizationalUnits' defined in yaml for the tf module '{module_name}' without any OUs, "
                            f"consider removing if not required."
                        )
                case "accounts":
                    account_targets = deployment_target.get("accounts")
                    if account_targets:
                        deployment_target_accounts.update(
                            account_ids_from_name(
                                deployment_target_accounts=account_targets,
                                ou_org_accounts=all_org_accounts_copy
                            )
                        )
                    else:
                        logger.warning(
                            f"'accounts' defined in yaml for the tf module '{module_name}' without any accounts, "
                            f"consider removing if not required."
                        )
        if dependencies:
            for dependent in dependencies:
                try:
                    if dependent not in all_modules:
                        raise Exception(
                            f"dependencies defined in the yaml for tf module '{module_name}' which don't exist")
                except Exception as e:
                    return logger.exception(f"'{e}'")

        logger.info(f"Accounts to deploy to for module {module_name}: {deployment_target_accounts}")
        create_provider_code(
            account_ids_by_name=account_ids_by_name,
            regions=ALL_REGIONS
        )
        create_module_code(
            module_name=module_name,
            module_source=module_source,
            deployment_account_ids=deployment_target_accounts,
            regions=regions,
            variables=variable_inputs,
            module_dependencies=dependencies
        )
    return


def main(test_debug_mode=False) -> None:
    if test_debug_mode:
        logger.setLevel("DEBUG")
        tf_config_template = load_configuration(yaml_file_name=DEBUG_YAML_FILE_NAME)
    else:
        tf_config_template = load_configuration(yaml_file_name=YAML_FILE_NAME)

    boto_org_config = Config(
        retries={
            'max_attempts': 10,
            'mode': 'standard'
        }
    )
    org_client = boto3.client(
        service_name="organizations",
        config=boto_org_config
    )
    tf_modules = tf_config_template.get("terraformModules")
    org_root_id = get_root_id(org_client=org_client)

    create_backend_code()

    if not tf_modules:
        logger.warning("No modules defined. Preparing for Terraform destroy operation or no module deployment required")
        # Get all accounts and regions from the organization
        all_accounts = get_all_accounts(org_client=org_client)
        all_regions = get_all_regions()
        create_provider_code(
            account_ids_by_name=all_accounts,
            regions=all_regions
        )
        create_empty_main_tf()
        return

    deploy_targets(
        org_client=org_client,
        tf_modules=tf_modules,
        root_id=org_root_id
    )
    return


if __name__ == "__main__":
    main()
