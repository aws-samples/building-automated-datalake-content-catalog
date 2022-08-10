# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0


import traceback
import boto3
import os
import logging
import json
import urllib.parse
from utils.common import get_s3_content 
import awswrangler as wr
import glob


logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.getLogger("awswrangler").setLevel(logging.DEBUG)

s3 = boto3.resource('s3')

def main(event, context):
    logger.info('Starting ETL State Machine Get Function')
    bucket = event['bucket']
    key = event['key']
    return handle_file(bucket, key)

def handle_file(bucket, key):
    data_spec_label = 'data_spec.json'
    file = f"s3://{bucket}/{key}"
    res = {
        "FileIn": file,
        "SpecFile": False,
        "FilesToProcess": [],
        "DataSpec": {}
    }
    if data_spec_label in file:
        data_spec_content = get_s3_content(bucket, key)
        if data_spec_content is None:
            return res
        data_spec = json.loads(data_spec_content)
        files_to_process = [f for f in wr.s3.list_objects(path=f"s3://{bucket}", suffix=['.csv'], ignore_suffix=data_spec_label) if f"/input/{data_spec['prefix_directory']}/" in f]
        res = update_res(files_to_process, data_spec, res)
    else:
        data_spec_key = get_lowest_prefix(key) + f'/{data_spec_label}'
        data_spec_content = get_s3_content(bucket, data_spec_key)
        # walk backwards to find spec file...
        while data_spec_content is None:
            data_spec_key_set = set()
            # Remove label before climbing folders
            data_spec_key = get_lowest_prefix(data_spec_key)
            data_spec_key = get_lowest_prefix(data_spec_key) + f'/{data_spec_label}'
            logger.info(data_spec_key)
            if data_spec_key == f'/{data_spec_label}' or data_spec_key in data_spec_key_set :
                break
            data_spec_key_set.add(data_spec_key)
            data_spec_content = get_s3_content(bucket, data_spec_key)
        if data_spec_content is None:
            return res
        data_spec = json.loads(data_spec_content)
        res = update_res([file], data_spec, res)

    return res

def get_lowest_prefix(key):
    return key.rpartition('/')[0]

def update_res(data_file, spec, res):
    res['SpecFile'] = True
    del res['FilesToProcess']
    res['FilesToProcess'] = data_file
    res['DataSpec'] = spec
    return res
