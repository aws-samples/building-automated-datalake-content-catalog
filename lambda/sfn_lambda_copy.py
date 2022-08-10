# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from sys import prefix
import traceback
import boto3
import os
import logging
import json
from datetime import date, datetime
import awswrangler as wr
import pandas as pd
import numpy as np
import urllib.parse

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.resource('s3')

DST_BUCKET_NAME = os.environ.get('DST_BUCKET_NAME')
SRC_BUCKET_NAME = os.environ.get('SRC_BUCKET_NAME')


def main(event, context):
    logger.info(event)
    logger.info('Starting ETL State Machine Copy Function')
    files = event['FilesToProcess']
    if len(files) == 0:
        return
    for f in files:
        file_to_copy = f.replace('/input/', '/transformed/') if event['DataSpec']['transform_job_name'] != False else f
        copy_file_to_data_lake(file_to_copy)
    data_spec = event['DataSpec']
    analysis = data_spec['statistical_analysis_job_name']
    if analysis != False:
        prefix = data_spec['prefix_directory']
        update_data_stats(prefix)
    update_data_catalog(data_spec)


def copy_file_to_data_lake(file):
    file = file.replace('s3://', '').split('/', 1)
    bucket = file[0]
    key = file[1]
    copy_source = {
        'Bucket': bucket,
        'Key': key
    }
    bucket = s3.Bucket(DST_BUCKET_NAME)
    obj = bucket.Object(key.replace('transformed', 'input'))
    obj.copy(copy_source)


def get_list_of_attribute_values(df, name, col=None):
    if col is not None:
        rows = df.loc[(df['attribute'] == name) & (df['column'] == col)]
    else:
        rows = df.loc[df['attribute'] == name]
    return rows['value'].tolist()


def merge_data_stats(df, prefix):
    cols = ['date', 'source', 'column', 'type', 'attribute', 'value', 'sample']
    rows = sum(get_list_of_attribute_values(df, 'rows'))
    columns = get_list_of_attribute_values(df, 'columns')[0]
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_df = pd.DataFrame({
        'date': [now] * 2,
        'source': [prefix] * 2,
        'column': [np.nan] * 2,
        'type': ['int64'] * 2,
        'attribute': ['rows', 'columns'],
        'value': [rows, columns],
        'sample': ['-'] * 2
    }, columns=cols)
    column_info = df.drop_duplicates('column', keep='first')
    column_base_list = []

    def append_to_base_list(col, cdtype, attr, val, sample='-'):
        column_base_list.append([now, prefix, col, cdtype, attr, val, sample])

    for _, row in column_info.iterrows():
        col = row['column']
        # Dont process rows that are specific to entire data set (Ex. row count of file)
        if not isinstance(col, str):
            continue
        cdtype = row['type']
        sample = row['sample']
        if cdtype != 'object':
            c_sum = sum(get_list_of_attribute_values(df, 'Sum', col))
            append_to_base_list(col, cdtype, 'Sum', c_sum, sample)
            c_mean = c_sum / rows
            append_to_base_list(col, cdtype, 'Mean', c_mean, sample)
            mins = get_list_of_attribute_values(df, 'Min', col)
            if mins:
                c_min = min(mins)
                append_to_base_list(col, cdtype, 'Min', c_min, sample)
            maxs = get_list_of_attribute_values(df, 'Max', col)
            if maxs:
                c_max = max(maxs)
                append_to_base_list(col, cdtype, 'Max', c_max, sample)
        c_non_null = sum(get_list_of_attribute_values(df, 'Non-Null Count', col)) / rows
        append_to_base_list(col, cdtype, 'Non-Null Count', c_non_null, sample)
    column_df = pd.DataFrame(column_base_list, columns=cols)
    return pd.concat([new_df, column_df])


def update_data_stats(prefix):
    all_analysis_path = f"s3://{SRC_BUCKET_NAME}/analyzed/{prefix}/"
    all_analysis_df = wr.s3.read_csv(path=all_analysis_path)
    analysis_df = merge_data_stats(all_analysis_df, prefix)
    path_out = f"s3://{DST_BUCKET_NAME}/input/stats/data_stats.csv"
    curr_stats_df = wr.s3.read_csv(path=path_out)
    final_stats_df = curr_stats_df.append(analysis_df)
    logger.info(f"Adding new data stats...")
    res = wr.s3.to_csv(
        df=final_stats_df,
        path=path_out,
        index=False
    )
    logger.info('Adding new data stats result: ', res)


def update_data_catalog(data_spec):
    logger.info(data_spec)
    metadata_df = pd.DataFrame(data_spec, index=[0])
    path_out = f"s3://{DST_BUCKET_NAME}/input/catalog/data_catalog.csv"
    curr_stats_df = wr.s3.read_csv(path=path_out)
    final_stats_df = curr_stats_df.append(metadata_df)
    final_stats_df = final_stats_df.drop_duplicates(keep='last')
    logger.info(f"Adding new data catalog entry...")
    res = wr.s3.to_csv(
        df=final_stats_df,
        path=path_out,
        index=False
    )
    logger.info('Adding new data catalog entry result: ', res)
