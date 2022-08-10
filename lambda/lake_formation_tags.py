# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import boto3

client = boto3.client('lakeformation')


def on_event(event, context):
    print(event)
    request_type = event['RequestType'].lower()
    if request_type == 'create':
        return on_create(event)
    if request_type == 'update':
        return on_update(event)
    if request_type == 'delete':
        return on_delete(event)
    raise Exception(f'Invalid request type: {request_type}')


def on_create(event):
    response_data = {}
    try:
        props = event["ResourceProperties"]
        print("Creating new resource with props %s" % props)
        tags = props["tags"]
        failure = False

        for tag in tags:
            if failure:
                response_data["Status"] = "FAILED"
                return response_data
            response = client.create_lf_tag(
                TagKey=tag["Name"],
                TagValues=tag["Values"]
            )
            if response["ResponseMetadata"]['HTTPStatusCode'] == 200:
                physical_id = tag["Name"]       # TODO: Change for actual resource physical ID when response have it
                response_data["PhysicalResourceId"] = physical_id
                response_data["Status"] = "SUCCESS"
            else:
                print('Failed creating LF Tag')
                failure = True
    except Exception as e:
        print('Exception on create')
        print(e)
        response_data["Status"] = "FAILED"
    print('Response data')
    print(response_data)
    return response_data


def on_update(event):
    response_data = {}
    try:
        physical_id = event["PhysicalResourceId"]
        props = event["ResourceProperties"]
        print("Updating resource %s with props %s" % (physical_id, props))
        response = client.list_lf_tags(
            ResourceShareType='ALL',
            MaxResults=100
        )
        for tag in response["LFTags"]:
            delete_response = client.delete_lf_tag(
                TagKey=tag["TagKey"]
            )
        response_data = on_create(event)
    except Exception as e:
        response_data["Status"] = "FAILED"
        print('Exception on update')
        print(e)
    return response_data


def on_delete(event):
    response_data = {}
    try:
        failure = False
        physical_id = event["PhysicalResourceId"]
        props = event["ResourceProperties"]
        print("delete resource %s" % physical_id)
        tags = props["tags"]
        for tag in tags:
            response = client.delete_lf_tag(
                TagKey=tag["Name"]
            )
            print(response)
            if response["ResponseMetadata"]['HTTPStatusCode'] == 200 and not failure:
                # physical_id = tag["Name"]       # TODO: Change for actual resource physical ID when response have it
                response_data["PhysicalResourceId"] = physical_id
                response_data["Status"] = "SUCCESS"
            else:
                response_data["Status"] = "FAILED"
                failure = True
        response_data["Status"] = "SUCCESS"
    except Exception as e:
        response_data["Status"] = "FAILED"
        print('Exception on delete')
        print(e)
    return response_data



