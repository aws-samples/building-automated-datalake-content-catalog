This project is a <b>prototype.</b> If you decided to deploy this work in production environment, please conduct your own security reviews and incorporate security controls commensurate with the data classification whitin your organization. (e.g., when S3 is created they should check to make sure BPA is enabled and/or encryption is enabled based on data sensitivity).

# Building an Automated Datalake Content Catalog

This Solution used the AWS Cloud Development Kit (CDK) to deploy an Infrastructure that showcase the use of Data Lake in a multi-account design. 

![alt Datalake Content Catalog](DataContentCatalogDiagram.drawio.png)
Figure 1: Multi-account Data catalog illustration

![alt Datalake Content Catalog](DataProcessing.drawio.png)
Figure 2: Internal processing for new incoming data

## Prerequisites

### 1.  Prerequisites Installation
   1. Install  [AWS Cloud Development Kit (CDK)](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html).
   2. Install [Node npm installer](https://docs.npmjs.com/downloading-and-installing-node-js-and-npm)
### 2.  Install Typescript and dependencies
```sh
npm update
npm install -g typescript
```

### 3.  Install [Docker](https://docs.docker.com/get-docker/)

### 4.  Create AWS Accounts
The following AWS accounts are required to demonstrate how distinct, isolated AWS environments can be used to secure the 
different functions of producing data, managing a data lake, and consuming data from the data lake:

* Data Producer Account
* Data Lake Account

### 5.  Configure temporary credentials to access AWS accounts

In order to use the CDK to deploy the cloud resources required by this prototype, you'll need to obtain temporary credentials for each of the accounts.

Since the scripts in this prototype require the use of AWS CLI profiles, you will need to ensure that a profile exists for 
each account in your `~/.aws/config` file and that temporary credentials are associated with each profile.

* See example on how to set up [Named profiles for the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html)

* AWS SSO: If you're using AWS SSO to manage access to your AWS accounts, you can use the AWS SSO support for AWS CLI access. 
See [Configuring the AWS CLI to use AWS Single Sign-On](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso.html) for instructions to set up profiles.

Each of these profiles requires `AdministratorAccess` to each of the accounts.

At the end of setting up your profiles, you shouldYou should end up with at three profiles. For example:

* `data-producer`
* `data-lake`

## Installation

### 1. Update Configurations - Step 1
1. Update **Makefile** to specify the AWS account IDs and associated profile names for each account.

| Variable                | Description                                             |
|-------------------------|---------------------------------------------------------|
| `DATA_LAKE_ACCOUNT`     | Data Lake AWS account ID                                |
| `DATA_LAKE_PROFILE`     | Data Lake AWS CLI profile name                          |
| `DATA_PRODUCER_ACCOUNT` | Data Producer AWS account ID                            |
| `DATA_PRODUCER_PROFILE` | Data Producer AWS CLI profile name                      |
| `REGION`                | AWS Region in which to deploy the prototype's resources |

2. Update **config/DataMarketPlaceConfig.yaml**

Edit `config/DataMarketPlaceConfig.yaml` and set the following keys:

| Variable              | Description                                                 |
|-----------------------|-------------------------------------------------------------|
| `AppPrefix`           | a prefix to append at the begins of all deployed resources. |
| `DataProducerAccount` | Data Producer AWS account ID                                |
| `DataLakeAccount`     | Data Lake AWS account ID                                    |
 | `NotificationEmail`   | Your Email address for notifications                        |   
### 2. Boostrap accounts
Run the below commands to cdk boostrap each account
```shell
make bootstrap-producer 
```
```shell
make bootstrap-datalake
```
### 3. Deploy Data Producer Account
* Deploy data producer stack
```sh
make deploy-data-producer
```

### 4. Update Configurations - Step 2
* From the stack output, copy the values for the following fields into the file `config/DataMarketPlaceConfig.yaml`

| Stack Output                          | Copy to             | Description                                                                    |
|---------------------------------------|---------------------|--------------------------------------------------------------------------------|
| `DataProducerStack.refineddatabucket` | `RegisteredBuckets` | This field takes a list of S3 bucket that hosts the refined data               |
| `DataProducerStack.s3bucketkmskeyarn` | `BucketKeyArn`      | This the the KMS key used to encrypt the `DataProducerStack.refineddatabucket` |

### 5.  Deploy Data Lake Account

* Deploy datalake stack:
```sh
make deploy-datalake
```

* Configure data lake policy
Execute a Python script to configure the Glue catalog permissions in the Data Lake account.
```sh
make set-datalake-policy
```
### 6. Cleanup
In order to clean the account run the below command:

```shell
make destroy-all
```

## Operational Guide
### 3. Grant Permissions to Consumer Account/Role

1. Go to the AWS Management Console. Access `Lake formation` -> `Data catalog` -> `Databases`
2. Select the database, select `Actions` -> `Edit`
3. Uncheck the `Use only IAM access control for new tables in this database` and select `Save`
4. Go to the AWS Management Console. Access `Lake formation` -> `Data lake permissions`
5. Select `Grant`
6. Select `External account` and type your consumer account ID. Press enter to add it
7. Select `Named data catalog resources`
8. Select the existing database from the list.
9. Select `All tables`
10. Select `Table permissions` - > check `Select` and `Describe`
11. Select `Grant`

### 4.  Deploy Resources to the Data Consumer Account

* In console, go to Resource Access Manager
* Look for resource shares and accept the invitation

### V. Test Deployments

### 1. Subscribe to data product from Market Data Exchange (ADX) and Set up Automated Data Ingestion
* Navigate to S3 `DataProducerStack.rawdatabucket`
* Create the following folder
  * `input/`
* Create a folder for each data provider/category
  * eg: `input/covid/`
* Go to the AWS Management Console. Access `AWS Data Exchange` and subscribe to the data of your interest
* Set up the location your data provider should send the data to
  * This should be the `DataProducerStack.rawdatabucket` S3 bucket
  * make sure you select the correct location/folder you created on step above
* Create a `data_spec.json` file for each data provider/category 
  * See example template from `/templates/data_spec.json`
  * To find the *TransformJobName* and *StatisticalAnalysisJobName* Go to the AWS Management Console. Access -> `AWS Glue` -> `Jobs` -> `Copy Job Name`
    * eg: *TransformJobName* : glueETLJobCBB5EB32-cIJRhmYnW8RD
    * eg: *StatisticalAnalysisJobName* : glueStatsJobB1B10E47-yyELMl0eRMlB
```
{
"Vendor": "AWS",
"DataType": "location",
"Geography": "US",
"ContractStart": "11/19/21",
"ContractEnd": "01/01/22",
"PrimaryContact": "your-contact-here",
"PrefixDirectory": "covid",
"TransformJobName": "Glue ETL Job Name",
"StatisticalAnalysisJobName": "Glue Stats Job Name",
"DataClassification": "Standard",
"DataAccessLevel": "Test"
}
```
* Upload the data file together with the `data_spec.json` to the corresponding folder of each provider/category
  * eg: upload *covid* data files and `data_spec.json` to `input/covid/`

### 2. Manually Execute Glue Crawler in Data Lake Account

1. In the data lake account, go to the AWS Management Console. Access `Lake formation` -> `Crawlers`
2. Select `glueCrawler_0`
3. Select `Run crawler`
4. Monitor progress of the crawler.
5. If successful, navigate back to Lake Formation
6. Validate that the expected tables appear.

### VI. Set up Consumer Account to Access Centrally Governed Tables

### 1. Create S3 Bucket to House Athena Query Results

1. Create S3 bucket. 
   1. Go to the AWS Management Console. Access `S3` and create a bucket
2. Go to the AWS Management Console. Access Athena `Query editor` -> `Settings`
   1. Select `Manage`
   2. Select `Browse S3` and select the query results bucket

### Create local Glue database

1. In the data consumer account, go to the AWS Management Console. Access `Glue` -> `Databases`
   1. Select `Add database`
   2. Enter `local` as the database name
   3. Select `Create`

### 2. Link Centrally Governed Tables to the Local Glue Database

1. Access `Lake Formation` -> `Tables`
2. For each table of interest:
  1. Select the table
  2. Select `Actions` -> `Create resource link`
  3. Enter the name of the original table and append it with `_link`
  4. Select the `local` database
  5. Select `Create`
3. Access `Tables`
4. Select a table and select `Actions` -> `View data`
  1. If this is the first time using Athena, select `View settings` to select up an S3 bucket for query results.  

### 3. Accessing Data via Athena in the Data Consumer Account
From here you can run your queries to access the data. For example
```sh
SELECT * FROM catalog
SELECT * FROM stats
```
...

## Appendix

### Useful commands

 * `npm run build`   compile typescript to js
 * `npm run watch`   watch for changes and compile
 * `npm run test`    perform the jest unit tests
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk synth`       emits the synthesized CloudFormation template
 
## Manual Deployment Assume role for the prototype account
```bash
Configure your AWS CLI using Profile
```
Generate the stack desired.
```bash
cdk synth -c config=DataProducer
cdk deploy -c config=DataProducer
```
or 
```bash
cdk synth -c config=DataLake
cdk deploy -c config=DataLake
```