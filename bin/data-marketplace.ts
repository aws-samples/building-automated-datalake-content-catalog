// Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

import 'source-map-support/register';
import {App, Aspects} from 'aws-cdk-lib';
import {DataMarketPlaceConfig, getConfig} from '../lib/DataMarketPlaceConfig'
import {DataProducerStack} from '../lib/data-producer-stack';
import {DataLakeStack} from '../lib/data-lake-stack';
// import {AwsSolutionsChecks} from 'cdk-nag'

const app = new App();

// Aspects.of(app).add(new AwsSolutionsChecks({ verbose: true }))
let env = app.node.tryGetContext('config');
let region = app.node.tryGetContext('region');
console.log("Deploy " + env + " To Region: " + region);

let marketPlaceConfig: DataMarketPlaceConfig = getConfig();

if (env == 'DataLake') {
	new DataLakeStack(app, 'DataLakeStack', marketPlaceConfig, {
		env: {
			region: region || process.env.CDK_DEFAULT_REGION,
			account: process.env.CDK_DEFAULT_ACCOUNT,
		},
	});
}
else if (env == 'DataProducer') {
	new DataProducerStack(app, 'DataProducerStack', marketPlaceConfig, {
		env: {
			region: region || process.env.CDK_DEFAULT_REGION,
			account: process.env.CDK_DEFAULT_ACCOUNT,
		},
	});
}