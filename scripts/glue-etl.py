# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import awswrangler as wr
from datetime import datetime
import pandas as pd
import os, sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

## ARGS
## bucket_out: output bucket
## file_in: input file

def crunchbase_merge(df):
    cols = ["alias1", "alias2", "alias3"]
    df["aliases"] = df[cols[0]].astype(str) + df[cols[1]].astype(str) + df[cols[2]].astype(str)
    df = df.drop(columns=cols)
    return df

def default_drop(df):
    if 'country' in df.columns:
        df = df.drop(columns=['country'])
    return df

sc = SparkContext.getOrCreate()
sc.setLogLevel("INFO")
glueContext = GlueContext(sc)
logger = glueContext.get_logger()
path = '/transformed/'

logger.info(f'Before resolving options...')

args = getResolvedOptions(sys.argv,
                          ['bucket_out',
                           'file_in'])

logger.info(f'Resolved options are: {args}')

file_in = args['file_in']
logger.info(f"Reading data from file: {file_in} ")
df = wr.s3.read_csv(path=file_in)

logger.info("Transforming data...")
if '/crunchbase/' in file_in:
    df = crunchbase_merge(df)
    path = path + 'crunchbase/'
else:
    df = default_drop(df)

path_out = file_in.replace('input', 'transformed')

logger.info(f"Writing data, to data lake...")
res = wr.s3.to_csv(
    df=df,
    path=path_out,
    index=False
)