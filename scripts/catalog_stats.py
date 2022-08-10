# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from pandas.core import base
import awswrangler as wr
from datetime import date, datetime
import pandas as pd
import numpy as np
import os, sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

## ARGS
## bucket_out: output bucket
## file_in: input file

def build_stats_df(df, file_in):
    base_list = []
    def append_to_base_list(col, cdtype, attr, val, sample='-'):
        base_list.append([ col, cdtype, attr, val, sample ])
    cols = ['column','type','attribute','value','sample']
    shape_x, shape_y = df.shape
    new_df = pd.DataFrame({
        'column': [np.nan] * 2,
        'type': ['int64'] * 2,
        'attribute': ['rows', 'columns'],
        'value': [shape_x, shape_y],
        'sample': ['-'] * 2
    }, columns=cols)
    for column in df:
        c_df = df[column]
        sample = c_df.sample(n=1).iloc[0]
        cdt = c_df.dtype.name
        if cdt != 'object':
            append_to_base_list(column, cdt, 'Mean', c_df.mean(), sample)
            append_to_base_list(column, cdt, 'Min', c_df.min(), sample)
            append_to_base_list(column, cdt, 'Max', c_df.max(), sample)
            append_to_base_list(column, cdt, 'Sum', c_df.sum(), sample)
        append_to_base_list(column, cdt, 'Non-Null Count', c_df.count(), sample)
    describe_df = pd.DataFrame(base_list, columns=cols)
    
    return pd.concat([new_df, describe_df])

sc = SparkContext.getOrCreate()
sc.setLogLevel("INFO")
glueContext = GlueContext(sc)
logger = glueContext.get_logger()

logger.info(f'Before resolving options...')

args = getResolvedOptions(sys.argv,
                          ['bucket_out',
                           'file_in'])

logger.info(f'Resolved options are: {args}')

file_in = args['file_in']
logger.info(f"Reading data from file: {file_in} ")
df = wr.s3.read_csv(path=file_in)
logger.info("Getting stats on data...")
new_stats_df = build_stats_df(df, file_in)
path_out = file_in.replace('input', 'analyzed')
# path_out = f"s3://{args['bucket_out']}{path}catalog_stats.csv"
# curr_stats_df = wr.s3.read_csv(path=path_out)
# ## append new stats to curr stats
# final_stats_df = curr_stats_df.append(new_stats_df)
## write new df to s3
logger.info(f"Writing new stats data to stats catalog...")
res = wr.s3.to_csv(
    df=new_stats_df,
    path=path_out,
    index=False
)