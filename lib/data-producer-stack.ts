// Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
import * as sam from 'aws-cdk-lib/aws-sam';
import * as path from "path";
import * as cdk from 'aws-cdk-lib';
import {RemovalPolicy} from 'aws-cdk-lib';
import * as kms from 'aws-cdk-lib/aws-kms';
import * as s3 from 'aws-cdk-lib/aws-s3';
import {BlockPublicAccess, BucketEncryption} from 'aws-cdk-lib/aws-s3';
import * as logs from 'aws-cdk-lib/aws-logs';
import {RetentionDays} from 'aws-cdk-lib/aws-logs';
import * as glue from '@aws-cdk/aws-glue-alpha';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import {ILayerVersion} from 'aws-cdk-lib/aws-lambda';
import {S3EventSource} from 'aws-cdk-lib/aws-lambda-event-sources';
import * as iam from 'aws-cdk-lib/aws-iam'
import {Effect, PolicyStatement, Role} from 'aws-cdk-lib/aws-iam'
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as subscriptions from 'aws-cdk-lib/aws-sns-subscriptions';
import {DataMarketPlaceConfig} from "./DataMarketPlaceConfig";

export class DataProducerStack extends cdk.Stack {
  public readonly Config: DataMarketPlaceConfig;
  constructor(scope: cdk.App, id: string, marketPlaceConfig:DataMarketPlaceConfig, props?: cdk.StackProps) {
    super(scope, id, props);

    // print configurations
    console.log(marketPlaceConfig)
    this.Config = marketPlaceConfig

    // Create KMS key we will use to encrypt the cluster data.
    const KMSKey = this.createKMSKey();
    const refined_data_bucket = this.createBucket('RefinedDataBucket', KMSKey);
    const raw_data_bucket = this.createBucket('RawDataBucket', KMSKey);

    // Add Data Exchange permissions to raw bucket
    raw_data_bucket.addToResourcePolicy(new iam.PolicyStatement(
      {
        sid: 'Allow AWS Data Exchange to use this Bucket',
        principals:[new iam.ServicePrincipal('dataexchange.amazonaws.com')],
        actions: [
          "s3:PutObject",
          "s3:PutObjectAcl"
         ],
        effect: Effect.ALLOW,
        resources: [raw_data_bucket.bucketArn + "/*"],
        conditions: {
          'StringEquals': {
            'aws:SourceAccount': cdk.Stack.of(this).account
          }
        }
      }
    ))

    // Put data_stats template into bucket
    this.deployDataTemplate('catalogDataStatsTemplate', refined_data_bucket, 'data_stats.csv', 'input/stats');

    // Put data_catalog template into bucket
    this.deployDataTemplate('catalogDataCatalogTemplate', refined_data_bucket, 'data_catalog.csv', 'input/catalog');

    // Create glue service role
    const glueServiceRole = this.createGlueServiceRole("glueServiceRole", KMSKey, refined_data_bucket, raw_data_bucket);

    // Create Glue ETL Job
    this.createGlueJob('glueETLJob',
        'Transformation job for raw data to refined data',
        glueServiceRole,
        '/../scripts/glue-etl.py',
        {
          '--bucket_out': raw_data_bucket.bucketName,
          '--additional-python-modules': 'pyarrow==2,awswrangler'
        },
        KMSKey
    );

    // Create glue job to produce catalog_stats
    const statsGlueJob = this.createGlueJob('glueStatsJob',
      'Job to extract stats on the new catalog entry',
      glueServiceRole,
      '/../scripts/catalog_stats.py',
      {
        '--bucket_out': raw_data_bucket.bucketName,
        '--additional-python-modules': 'pyarrow==2,awswrangler'
      },
      KMSKey
    )

    // Create lambda role
    const lambda_role = this.createLambdaRole('documentsLambdaRole', KMSKey, refined_data_bucket, raw_data_bucket)

    // Create lambda to trigger glue crawler + stat catalog update when new file is created in input directory
    const documents_lambda = this.createDocumentLambdaHandler('documentsLambda', refined_data_bucket, lambda_role)
    documents_lambda.addEnvironment('JOB_NAME', statsGlueJob.jobName);

    // create ETL Statemachine flow and lambda to trigger ETL state machine
    const etlMachine = this.createETLWorkflow(KMSKey, raw_data_bucket, refined_data_bucket, lambda_role);
    const etl_lambda = this.createETLLambdaHandler('etlLambda', raw_data_bucket, etlMachine)
    etlMachine.grantStartExecution(etl_lambda);

    // Create outputs
    new cdk.CfnOutput(this, 'raw_data_bucket', {
      value: raw_data_bucket.bucketName
    });

    new cdk.CfnOutput(this, 'refined_data_bucket', {
      value: refined_data_bucket.bucketName
    });

    new cdk.CfnOutput(this, 's3_bucket_kms_key_arn', {
      value: KMSKey.keyArn
    });
  }

  private createKMSKey() {
     const key = new kms.Key(this,  this.Config.AppPrefix + '-Key',
        {
          enableKeyRotation: true,
          alias: 'DataProducerKey',
          removalPolicy: cdk.RemovalPolicy.DESTROY,
          pendingWindow: cdk.Duration.days(7)
        }
    );

    key.addToResourcePolicy(new iam.PolicyStatement(
        {
          sid: 'Allow use of the key',
          principals: [new iam.CompositePrincipal(
              new iam.ServicePrincipal('logs.amazonaws.com'),
              new iam.ServicePrincipal('glue.amazonaws.com'),
              new iam.AccountPrincipal(this.Config.DataLakeAccount)
          )],
          actions: ['kms:Decrypt', 'kms:Encrypt', 'kms:ReEncrypt*', 'kms:GenerateDataKey*', "kms:DescribeKey"],
          effect: Effect.ALLOW,
          resources: ["*"]
        }
    ));
    return key;
  }

  private createBucket(bucket_name: string, kms_key:kms.IKey) {
    const bucket = new s3.Bucket(this, bucket_name, {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      encryptionKey: kms_key,
      encryption: BucketEncryption.KMS,
      autoDeleteObjects: true,
      serverAccessLogsPrefix: 'access-logs',
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
    });

    bucket.addToResourcePolicy(new iam.PolicyStatement(
         {
           sid: 'Allow an external Lake Formation account to use this Bucket',
           principals:[new iam.AccountPrincipal(this.Config.DataLakeAccount)],
           actions: [
               "s3:GetBucketLocation",
               "s3:GetObject",
               "s3:ListBucket",
               "s3:ListBucketMultipartUploads",
               "s3:ListMultipartUploadParts",
           ],
           effect: Effect.ALLOW,
           resources: [bucket.bucketArn + "/*", bucket.bucketArn]}
     ))
    return bucket
  }

  private deployDataTemplate(identifier: string, destBucket: s3.Bucket, fileName: string, folderPrefix: string){
    const src = s3deploy.Source.asset(path.join(__dirname, '/../templates'), { exclude: ['**', `!${fileName}`]})
    new s3deploy.BucketDeployment(this, identifier, {
      destinationBucket: destBucket,
      sources: [src],
      destinationKeyPrefix: folderPrefix
    })
  }

  private createGlueServiceRole(identifier: string, kms_key:kms.IKey, src_bucket: s3.Bucket, dst_bucket: s3.Bucket) {
    const glue_role = new Role(this, identifier, {
      assumedBy: new iam.CompositePrincipal(
          new iam.ServicePrincipal('glue.amazonaws.com')
      ),
    });
    glue_role.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'))
    // glue_role.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess'))
    glue_role.addToPolicy(new PolicyStatement({
      actions: ['kms:Decrypt', 'kms:Encrypt', 'kms:GenerateDataKey'],
      effect: Effect.ALLOW,
      resources: [kms_key.keyArn]
    }));
    glue_role.addToPolicy(new PolicyStatement({
      actions: ['logs:AssociateKmsKey'],
      effect: Effect.ALLOW,
      resources: ["*"]
    }));
    glue_role.addToPolicy(new PolicyStatement({
      sid: "S3AccessPolicy",
      actions: [
        "s3:GetBucketLocation", "s3:GetObject", "s3:GetBucketAcl", "s3:PutObject", "s3:PutObjectAcl", "s3:ListBucket",
        "s3:ListBucketMultipartUploads", "s3:ListMultipartUploadParts",
      ],
      effect: Effect.ALLOW,
      resources: [
        src_bucket.bucketArn, src_bucket.bucketArn + "/*",
        dst_bucket.bucketArn, dst_bucket.bucketArn+ "/*"
      ]
    }));
    return glue_role
  }

  private createLambdaRole(identifier: string, kms_key:kms.IKey, src_bucket: s3.Bucket, dst_bucket: s3.Bucket){
    const lambda_role = new Role(this, identifier, {
      assumedBy: new iam.CompositePrincipal(
          new iam.ServicePrincipal('lambda.amazonaws.com')
      ),
    });
    lambda_role.addToPolicy(new PolicyStatement({
      actions: ['kms:Decrypt', 'kms:Encrypt', 'kms:GenerateDataKey'],
      effect: Effect.ALLOW,
      resources: [kms_key.keyArn]
    }));

    lambda_role.addToPolicy(new PolicyStatement({
      sid: "S3AccessPolicy",
      actions: [
        "s3:GetBucketLocation", "s3:GetObject", "s3:GetBucketAcl", "s3:PutObject", "s3:PutObjectAcl", "s3:ListBucket",
        "s3:ListBucketMultipartUploads", "s3:ListMultipartUploadParts",
      ],
      effect: Effect.ALLOW,
      resources: [
          src_bucket.bucketArn, src_bucket.bucketArn + "/*",
          dst_bucket.bucketArn, dst_bucket.bucketArn+ "/*"
      ]
    }));

    lambda_role.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'));
    lambda_role.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole'));

    return lambda_role;
  }

  private createGlueJob(identifier: string, description: string, role:Role, path_to_asset: string, default_args: any, kms_key: kms.Key){
    const securityConfig = new glue.SecurityConfiguration(this, identifier+'SecurityConfig', {
      securityConfigurationName: identifier + 'SecurityConfig',
      cloudWatchEncryption: {
        mode: glue.CloudWatchEncryptionMode.KMS,
        kmsKey: kms_key,
      },
      jobBookmarksEncryption: {
        mode: glue.JobBookmarksEncryptionMode.CLIENT_SIDE_KMS,
        kmsKey: kms_key,
      },
      s3Encryption: {
        mode: glue.S3EncryptionMode.KMS,
        kmsKey: kms_key,
      },
    });
    return new glue.Job(this, identifier, {
      executable: glue.JobExecutable.pythonEtl({
        glueVersion: glue.GlueVersion.V3_0,
        pythonVersion: glue.PythonVersion.THREE,
        script: glue.Code.fromAsset(path.join(__dirname, path_to_asset))
      }),
      description: description,
      role: role,
      defaultArguments: default_args,
      maxConcurrentRuns: 5,
      securityConfiguration: securityConfig,
    });
  }

  private createDocumentLambdaHandler(identifier: string, bucket: s3.Bucket, role:Role) {
    const lambda_handler = new lambda.Function(this, identifier, {
      runtime: lambda.Runtime.PYTHON_3_9,
      code: lambda.Code.fromAsset(path.join(__dirname, '/../lambda'), {bundling: DataProducerStack.get_lambda_bundle()}),
      handler: "documents_lambda.main",
      tracing: lambda.Tracing.ACTIVE,
      reservedConcurrentExecutions: 5,
      environment: {
        REGION: cdk.Stack.of(this).region
      },
      role: role
    });

    lambda_handler.addEventSource(new S3EventSource(bucket, {
      events: [ s3.EventType.OBJECT_CREATED],
      filters: [ { prefix: 'input/'} ]
    }))
    return lambda_handler
  }

   private createETLLambdaHandler(identifier: string, bucket: s3.Bucket, etlMachine: sfn.StateMachine) {
    const lambda_handler = new lambda.Function(this, identifier, {
      runtime: lambda.Runtime.PYTHON_3_9,
      code: lambda.Code.fromAsset(path.join(__dirname, '/../lambda'), {bundling: DataProducerStack.get_lambda_bundle()}),
      handler: "etl_lambda.main",
      tracing: lambda.Tracing.ACTIVE,
      reservedConcurrentExecutions: 5,
      environment: {
        REGION: cdk.Stack.of(this).region,
        STATE_MACHINE: etlMachine.stateMachineArn
      }
    });

    lambda_handler.addEventSource(new S3EventSource(bucket, {
        events: [ s3.EventType.OBJECT_CREATED],
        filters: [ { prefix: 'input/'} ]
    }))
    return lambda_handler
  }

  private createSnsTopicWithSubs(identifier: string, kms_key:kms.IKey){
    const topic = new sns.Topic(this, identifier, {
      masterKey: kms_key
    });
    topic.addSubscription(new subscriptions.EmailSubscription(this.Config.NotificationEmail))
    return topic;
  }

  private createETLWorkflow(kms_key: kms.IKey, src_bucket: s3.Bucket, dst_bucket: s3.Bucket, lambdaRole: Role) {
    const lambdaEnv = {
      SRC_BUCKET_NAME: src_bucket.bucketName,
      DST_BUCKET_NAME: dst_bucket.bucketName
    }
    const lambdaLayers = [ this.get_wrangler_layer() ];
    const getFilesFunc = new lambda.Function(this, 'sfnGetFilesFunc', {
      runtime: lambda.Runtime.PYTHON_3_9,
      code: lambda.Code.fromAsset(path.join(__dirname, '/../lambda'), {bundling: DataProducerStack.get_lambda_bundle(lambda.Runtime.PYTHON_3_9)}),
      layers: lambdaLayers,
      handler: "sfn_lambda_get.main",
      tracing: lambda.Tracing.ACTIVE,
      reservedConcurrentExecutions: 5,
      environment: lambdaEnv,
      role: lambdaRole,
      timeout: cdk.Duration.minutes(15)
    });
    const getFilesTask = new tasks.LambdaInvoke(this, 'Get Spec File', {
      lambdaFunction: getFilesFunc,
      outputPath: '$.Payload'
    });
    const copyFilesFunc = new lambda.Function(this, 'sfnCopyFilesFunc', {
      runtime: lambda.Runtime.PYTHON_3_9,
      code: lambda.Code.fromAsset(path.join(__dirname, '/../lambda'), {bundling: DataProducerStack.get_lambda_bundle(lambda.Runtime.PYTHON_3_9)}),
      layers: lambdaLayers,
      handler: "sfn_lambda_copy.main",
      tracing: lambda.Tracing.ACTIVE,
      reservedConcurrentExecutions: 5,
      environment: lambdaEnv,
      role: lambdaRole,
      timeout: cdk.Duration.minutes(15),
      memorySize: 512
    });
    const copyFilesTask = new tasks.LambdaInvoke(this, 'Copy To Data Lake', {
      lambdaFunction: copyFilesFunc,
      inputPath: '$'
    });
    const transformGlueJobTask = new tasks.GlueStartJobRun(this, 'Transform Glue Job', {
      glueJobName: sfn.TaskInput.fromJsonPathAt('$.DataSpec.transform_job_name').value,
      arguments:  sfn.TaskInput.fromObject({
        '--file_in': sfn.TaskInput.fromJsonPathAt('$.FileToProcess').value
      }),
      integrationPattern: sfn.IntegrationPattern.RUN_JOB,
      inputPath: '$',
      resultPath: sfn.JsonPath.DISCARD,
      outputPath: '$'
    });
    const analysisGlueJobTask = new tasks.GlueStartJobRun(this, 'Analysis Glue Job', {
      glueJobName: sfn.TaskInput.fromJsonPathAt('$.DataSpec.statistical_analysis_job_name').value,
      arguments:  sfn.TaskInput.fromObject({
        '--file_in': sfn.TaskInput.fromJsonPathAt('$.FileToProcess').value
      }),
      integrationPattern: sfn.IntegrationPattern.RUN_JOB,
      inputPath: '$',
      resultPath: sfn.JsonPath.DISCARD,
      outputPath: '$'
    });
    const dataSpecChoice = new sfn.Choice(this, 'Data Spec Found?');
    const dataSpecCondition = sfn.Condition.booleanEquals('$.SpecFile', false);
    const noDataSpecNotification = new tasks.SnsPublish(this, 'No Data Spec Notification', {
      message: sfn.TaskInput.fromObject({
        ProvidedFile: sfn.JsonPath.stringAt('$.FileIn')
      }),
      topic: this.createSnsTopicWithSubs('DataSpecNotFoundSns', kms_key),
      subject: 'Data Spec File Not Found!'
    })
    const dataSpecFail = new sfn.Fail(this, 'No Data Spec File Found');
    const filesMap = new sfn.Map(this, 'Iterate Files to Process', {
      maxConcurrency: 5,
      inputPath: '$',
      itemsPath: '$.FilesToProcess',
      parameters: {
        'FileToProcess': sfn.TaskInput.fromJsonPathAt('$$.Map.Item.Value').value,
        'DataSpec': sfn.TaskInput.fromJsonPathAt('$.DataSpec').value
      },
      resultPath: sfn.JsonPath.DISCARD,
      outputPath: '$'
    })
    const transformChoice = new sfn.Choice(this, 'Transform Data?');
    const transformCondition = sfn.Condition.booleanEquals('$.DataSpec.transform_job_name', false);
    const analysisChoice = new sfn.Choice(this, 'Analyze Data?');
    const analysisCondition = sfn.Condition.booleanEquals('$.DataSpec.statistical_analysis_job_name', false);
    const definition = getFilesTask
    /**
     * {
     *  Payload: {
     *    SpecFile: "s3://bucket/input/data_spec.json" || false,
     *    FileToProcess: "s3://bucket/input/file.csv" || false,
     *    DataSpec: { ... } || {}
     *  }
     * }
     */
      .next(dataSpecChoice
        .when(dataSpecCondition, noDataSpecNotification.next(dataSpecFail))
        .otherwise(filesMap
          .iterator(transformChoice
            .when(transformCondition, analysisChoice)
            .otherwise(transformGlueJobTask.next(analysisChoice
              .when(analysisCondition, new sfn.Pass(this, 'No Analysis to be done'))
              .otherwise(analysisGlueJobTask)
              )
            )
          ).next(copyFilesTask)
        )
      )
    const logGroup = new logs.LogGroup(this, 'ETLStateMachineLogGroup', {
      encryptionKey: kms_key,
      logGroupName: 'ETLStateMachineLogGroup',
      retention: RetentionDays.ONE_MONTH,
      removalPolicy: RemovalPolicy.DESTROY
    });
    const etlMachine = new sfn.StateMachine(this, 'ETLStateMachine', {
      definition: definition,
      stateMachineName: 'ETLStateMachine',
      tracingEnabled: true,
      logs: {
        destination: logGroup,
        level: sfn.LogLevel.ALL,
      }
    });
    etlMachine.role.addToPrincipalPolicy(new PolicyStatement({
      actions: [
        "glue:Get*",
        "glue:BatchGet*",
        "glue:StartJobRun",
        "glue:GetJobRun",
        "glue:GetJobRuns",
        "glue:BatchStopJobRun"
      ],
      effect: Effect.ALLOW,
      resources: [`arn:aws:glue:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:job/*`]
    }));
    etlMachine.role.addToPrincipalPolicy(new PolicyStatement({
      actions: ['kms:Decrypt', 'kms:Encrypt', 'kms:GenerateDataKey'],
      effect: Effect.ALLOW,
      resources: [kms_key.keyArn]
    }));
    return etlMachine;
  }

  private static get_lambda_bundle(runtime?: lambda.Runtime) : cdk.BundlingOptions {
    return {
      image: runtime?.bundlingImage ?? lambda.Runtime.PYTHON_3_9.bundlingImage,
      command: [
        "bash", "-c",
        "pip install --no-cache -r requirements.txt -t /asset-output && cp -au . /asset-output"
      ]
    }
  }

  private get_wrangler_layer(): ILayerVersion {
    const wrangler_layer = new sam.CfnApplication( this, "aws-data-wrangler-layer-py3-9", {
      location: {
        // 336392948345 is an official AWS Account (Ref: https://github.com/awslabs/aws-data-wrangler;
        // https://aws-data-wrangler.readthedocs.io/en/2.16.1/install.html#aws-lambda-layer)
        applicationId: "arn:aws:serverlessrepo:us-east-1:336392948345:applications/aws-data-wrangler-layer-py3-9",
        semanticVersion: "2.16.1"
      }
    });
    const wrangler_layer_arn = wrangler_layer.getAtt("Outputs.WranglerLayer39Arn").toString();
    return lambda.LayerVersion.fromLayerVersionArn(this, "wrangler-layer-version", wrangler_layer_arn);
  }

}
