# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class S3Uploadevent:
    def __init__(self, event):
        self._event = event

    @classmethod
    def from_json(cls, event):
        return cls(event=event)

    @property
    def bucket(self):
        return self._event["Records"][0]["s3"]["bucket"]

    @property
    def s3_object(self):
        return self._event["Records"][0]["s3"]["object"]

    @property
    def bucket_name(self):
        return self.bucket["name"]

    @property
    def object_key(self):
        return self.s3_object["key"]


def get_s3_content(bucket, key):
    s3 = boto3.client('s3')
    try:
        content_object = s3.get_object(Bucket=bucket, Key=key)
        return content_object['Body'].read().decode('utf-8')
    except Exception as e:
        return None


def read_json_file(bucket, filename):
    s3 = boto3.client('s3')
    content_object = s3.get_object(Bucket=bucket, Key=filename)
    file_content = content_object['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    return json_content


def write_json_file(bucket, sub_dir, filename, content):
    s3 = boto3.resource('s3')
    filepath = sub_dir + '/' + filename
    s3.Object(bucket, filepath).put(Body=json.dumps(content))
    logger.info("Writing file {0}".format(filepath))


def parse_object_key(object_key: str):
    key_split = object_key.split("/")
    file_name = key_split[-1]
    data_prefix = key_split[:-1]
    return file_name, data_prefix
