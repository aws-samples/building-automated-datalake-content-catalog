# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import json
import argparse

def main():
    parser = argparse.ArgumentParser(description='Update Glue Resource Policy')
    parser.add_argument('--region', help='The Region of Glue', required=True)
    parser.add_argument('--account', help='The Account of Glue', required=True)
    parser.add_argument('--profile', help='The Profile of Glue', required=True)
    parser.add_argument('--consumer', help='The Consumer account', required=True)
    try:
        args = parser.parse_args()

        region = args.region
        account = args.account
        profile = args.profile
        consumer = args.consumer
        print("Args: Region: {0}  Account:{1}  Consumers:{2}".format(region, account, profile, consumer))

        glue_session = boto3.session.Session(profile_name=profile, region_name=region)
        glue = glue_session.client('glue')
        
        statement_default = {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::" + account + ":root"
            },
            "Action": [
                "glue:*"
            ],
            "Resource": [
                f"arn:aws:glue:{region}:{account}:catalog",
                f"arn:aws:glue:{region}:{account}:database/default",
                f"arn:aws:glue:{region}:{account}:database/*"
            ]
        }

        statement1 = {
                    "Sid": "AllowConsumerFullCatalogAccess",
                    "Effect": "Allow",
                    "Action": [
                        "glue:*"
                    ],
                    "Principal": {
                        "AWS": "arn:aws:iam::" + consumer + ":root"
                    },
                    "Resource": [
                        f"arn:aws:glue:{region}:{account}:catalog",
                        f"arn:aws:glue:{region}:{account}:database/*",
                        f"arn:aws:glue:{region}:{account}:table/*/*"
                    ]
                }

        statement2 = {
                    "Sid": "AllowLFTagsFullCatalogAccess",
                    "Effect": "Allow",
                    "Action": [
                        "glue:*"
                    ],
                    "Principal": {
                        "AWS": "arn:aws:iam::" + consumer + ":root"
                    },
                    "Resource": [
                        f"arn:aws:glue:{region}:{account}:catalog",
                        f"arn:aws:glue:{region}:{account}:database/*",
                        f"arn:aws:glue:{region}:{account}:table/*/*"
                    ],
                    "Condition": {
                        "Bool": {
                            "glue:EvaluatedByLakeFormationTags": "true"
                        }
                    }
                }

        statement3 = {
            "Sid": "AllowRAMCatalogAccess",
            "Effect": "Allow",
            "Action": [
                "glue:ShareResource"
            ],
            "Principal": {
                "Service": "ram.amazonaws.com"
            },
            "Resource": [
                f"arn:aws:glue:{region}:{account}:catalog",
                f"arn:aws:glue:{region}:{account}:database/*",
                f"arn:aws:glue:{region}:{account}:table/*/*"
            ]
        }

        policy = {
            "Version": "2012-10-17",
            "Statement": [
                statement_default,
                statement1,
                statement2,
                statement3
            ]
        }

        policy_str = json.dumps(policy, indent=4)
        print(policy_str)
        glue.put_resource_policy(PolicyInJson=policy_str, EnableHybrid='TRUE')
        print("Policy updated.")

        # print("Changing Datalake default permissions ")
        # client_dl = boto3.client('lakeformation')
        # response = client_dl.put_data_lake_settings(
        #     DataLakeSettings={
        #         "DataLakeAdmins": [
        #             {
        #                 "DataLakePrincipalIdentifier": "arn:aws:iam::" + DATALAKE_ACCOUNT_ID + ":role/Admin"
        #             }
        #         ],
        #         "CreateDatabaseDefaultPermissions": [],
        #         "CreateTableDefaultPermissions": []
        #     }
        # )
        # print(response)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()

# python set_glue_policy.py --region us-east-2 --account 731946070856 --consumer-accounts 539142773658