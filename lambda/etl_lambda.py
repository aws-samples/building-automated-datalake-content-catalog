# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import traceback
import boto3
import os
import logging
import json
from utils.common import S3Uploadevent, write_json_file, parse_object_key
from urllib.parse import unquote_plus


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def main(event, context):
    logger.info('Starting ETL lambda main')
    s3_event = S3Uploadevent.from_json(event)
    bucket = s3_event.bucket_name
    key = unquote_plus(s3_event.object_key)
    filename, prefix = parse_object_key(key)
    logger.info(f'Bucket: {bucket} Key: {key}')
    if '.csv' not in key and 'data_spec.json' not in key:
        logger.info('Not a CSV or data_spec file! Not starting ETL Machine.')
        return 
    STATE_MACHINE = os.environ.get('STATE_MACHINE')
    try:
        sfn_client = boto3.client('stepfunctions')
        logger.info(f'## Starting ETL Machine: {STATE_MACHINE}')
        file_in = json.dumps({"bucket": bucket, "key": key})
        response = sfn_client.start_execution(
            stateMachineArn=STATE_MACHINE,
            input=file_in
        )
        logger.info(f'Response: \n{response}')
    except Exception as e:
        logger.error("Exception in ETL lambda main")
        logger.error(traceback.format_exc())
        logger.info("Attempting to write file to errors directory...")
        try:
            # attempt to write the original json to errors directory
            write_json_file(bucket, "errors", filename, event)
            logger.info("Error file created.")
        except Exception as err:
            logger.error("Failed to write original json to error directory. check input directory for {0}".format(key), err)

