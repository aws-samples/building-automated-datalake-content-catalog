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

    try:
        args = parser.parse_args()

        REGION = args.region
        RECEIVER_ACCOUNT_ID = args.account
        RECEIVER_PROFILE = args.profile

        print("Args: Region: {0}  Account:{1} ".format(REGION, RECEIVER_ACCOUNT_ID, RECEIVER_PROFILE))

        glue_session = boto3.session.Session(profile_name={RECEIVER_PROFILE})
        glue = glue_client = glue_session.client('glue')

        statement1 = {
                    "Sid": "AllowConsumerFullCatalogAccess",
                    "Effect": "Allow",
                    "Action": [
                        "ram:AcceptResourceShareInvitation",
                        "ram:RejectResourceShareInvitation",
                        "ec2:DescribeAvailabilityZones",
                        "ram:EnableSharingWithAwsOrganization"
                    ],
                    "Resource": "*"
                }

        policy = {
            "Version": "2012-10-17",
            "Statement": [
                statement1
            ]
        }

        policy_str = json.dumps(policy)
        print(policy_str)
        glue.put_resource_policy(PolicyInJson=policy_str)
        print("Policy updated.")
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()

# python set_glue_policy.py --region us-east-2 --account 111111111111111