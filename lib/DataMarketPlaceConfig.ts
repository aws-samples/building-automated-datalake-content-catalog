// Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

import * as fs from 'fs'
import * as path from "path";
const yaml = require('js-yaml');

export interface LakeFormationTag
{
    readonly Name: string;
    readonly Values: string[];
}

export interface DataMarketPlaceConfig
{
    readonly AppPrefix : string;
    readonly DataProducerAccount : string;
    readonly DataLakeAccount : string;
    readonly RegisteredBuckets : string[];
    readonly BucketKeyArn: string;
    readonly LakeFormationTags: LakeFormationTag[];
	readonly NotificationEmail: string;
}

export function getConfig()
{
    let unparsedConfig = yaml.load(fs.readFileSync(path.resolve("./config/DataMarketPlaceConfig.yaml"), "utf8"));
	let lakeFormationTags: LakeFormationTag[] = [];
	if ("LakeFormationTags" in unparsedConfig) {
		for (const element of unparsedConfig['LakeFormationTags']) {
			const tag_raw = element;
			const tag: LakeFormationTag = {
				Name: tag_raw["Name"],
				Values: tag_raw["Values"]
			}
			lakeFormationTags.push(tag)
		}
	}

	let config: DataMarketPlaceConfig = {
        RegisteredBuckets: unparsedConfig['RegisteredBuckets'],
		BucketKeyArn: unparsedConfig['BucketKeyArn'],
		AppPrefix: unparsedConfig["AppPrefix"],
		DataProducerAccount:unparsedConfig["DataProducerAccount"],
		DataLakeAccount: unparsedConfig["DataLakeAccount"],
		LakeFormationTags: lakeFormationTags,
		NotificationEmail: unparsedConfig["NotificationEmail"],
    };
    return config;
}