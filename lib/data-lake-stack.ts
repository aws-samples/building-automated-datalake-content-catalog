// Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

import * as cdk from "aws-cdk-lib";
import * as kms from 'aws-cdk-lib/aws-kms';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as lake_formation from 'aws-cdk-lib/aws-lakeformation';
import * as secrets_manager from 'aws-cdk-lib/aws-secretsmanager'
import * as logs from 'aws-cdk-lib/aws-logs';
import * as cr from 'aws-cdk-lib/custom-resources';
import * as iam from 'aws-cdk-lib/aws-iam'
import {Effect, ManagedPolicy, PolicyStatement, Role, ServicePrincipal} from 'aws-cdk-lib/aws-iam'
import {DataMarketPlaceConfig} from './DataMarketPlaceConfig'
import {CfnDataLakeSettings, CfnPermissionsProps} from "aws-cdk-lib/aws-lakeformation/lib/lakeformation.generated";
import * as glue from '@aws-cdk/aws-glue-alpha';
import * as glueL2 from 'aws-cdk-lib/aws-glue';
import * as path from "path";

export class DataLakeStack extends cdk.Stack {
    public readonly Config: DataMarketPlaceConfig;
    public readonly DataLakeKey: kms.IKey;
    public readonly DataLakeAdminRole: iam.IRole;
    public readonly LakeFormationSettings: lake_formation.CfnDataLakeSettings;
    private readonly LakeFormationServiceRole: iam.Role;
    public readonly LakeFormationResources: lake_formation.CfnResource[];

    public readonly DataAnalystRole1: iam.IRole;
    public readonly DataAnalystRole2: iam.IRole;
    public readonly DataAnalystPublicRole: iam.IRole;

  constructor(scope: cdk.App, id: string, marketPlaceConfig:DataMarketPlaceConfig, props?: cdk.StackProps) {
    super(scope, id, props);
    // Print configurations
    console.log(marketPlaceConfig)
    this.Config=marketPlaceConfig;

    // Create KMS for data lake account data encryption
    this.DataLakeKey = this.createKMSKey();

    // Create Glue DB for Lake Formation to store Metadata
    const glueDB = this.createGlueDatabase();

    // Create data lake admin role
    this.DataLakeAdminRole = this.createDataLakeAdminRole();

    // Create a role for custom resource for executing API's which are not supported by cloudformation
    const customResourceRole = this.createLakeFormationCustomResourceRole();

    // Setup Lake Formation settings with default permission for the account and admin role created
    this.LakeFormationSettings = this.createLakeFormationSettings([
          `arn:aws:iam::${cdk.Stack.of(this).account}:role/Admin`,
          `arn:aws:iam::${cdk.Stack.of(this).account}:role/cdk-mc-stack-cfn-exec-role-${cdk.Stack.of(this).account}-${cdk.Stack.of(this).region}`,
          this.DataLakeAdminRole.roleArn,
          customResourceRole.roleArn
      ],
    glueDB
    );
    // Create Lake Formation service role
    this.LakeFormationServiceRole = this.createLakeFormationServiceRole("lakeformation.amazonaws.com");

    // Lakeformation Register S3 buckets
    this.LakeFormationResources = this.datalakeRegisterS3buckets();

    // Create LakeFormation tags
    this.createLakeFormationTags(customResourceRole);

    // Create glue service role
    const glueServiceRole = this.createGlueServiceRole("glueServiceRole",
      "glue.amazonaws.com",
      ["service-role/AWSGlueServiceRole", "AmazonS3ReadOnlyAccess"]
    );

    // Create glue crawler to try to analyze new data files
    const glueCrawlers = this.createGlueCrawlers('glueCrawler',
        glueServiceRole.roleArn,
        glueDB.databaseName,
        this.Config.RegisteredBuckets
    );

    // For future usage invoking crawler automatically when file is dropped
    const crawlersSecret = this.createCrawlersSecret(glueCrawlers, this.DataLakeKey);

    // Create LakeFormation permissions
    this.grantCrawlerRolePermissions(glueServiceRole, glueDB);

    // Create Analysts roles
    this.DataAnalystRole1 = this.createLakeFormationAnalystRole('Analyst1', 'Analyst1');
    this.DataAnalystRole2 = this.createLakeFormationAnalystRole('Analyst2', 'Analyst2');
    this.DataAnalystPublicRole = this.createLakeFormationAnalystRole('Analyst-public', 'Analyst-public');

    new cdk.CfnOutput(this, 'lake_formation_role_output', {
      value: this.LakeFormationServiceRole.roleName
    });

    new cdk.CfnOutput(this, 'secret_output', {
      value: crawlersSecret.name?crawlersSecret.name:''
    });
  }

  private createKMSKey() {
      const key = new kms.Key(this,  this.Config.AppPrefix + '-Key',
          {
              enableKeyRotation: true,
              alias: 'DataLakeKey',
              removalPolicy: cdk.RemovalPolicy.DESTROY,
              pendingWindow: cdk.Duration.days(7)
          }
      );
       key.addToResourcePolicy(new iam.PolicyStatement(
         {
           sid: 'Allow an external Lake Formation account to use this KMS key',
           principals:[new iam.AccountPrincipal(this.Config.DataLakeAccount)],
           actions: ['kms:Decrypt', 'kms:Encrypt', 'kms:GenerateDataKey', "kms:DescribeKey"],
           effect: Effect.ALLOW,
           resources: ["*"]}
     ))
     return key;
  }

  private createLakeFormationSettings(adminArns:string[], glueDB: glue.Database) {
    let admins = Array<CfnDataLakeSettings.DataLakePrincipalProperty>();
    for (const element of adminArns) {
        admins.push({
        dataLakePrincipalIdentifier: element
      })
    }

    let settings = new lake_formation.CfnDataLakeSettings(this, "LakeFormationSettings", {
      admins: admins
    });

    const tableResourceProperty: lake_formation.CfnPermissions.TableResourceProperty = {
      catalogId: glueDB.catalogId,
      databaseName: glueDB.databaseName,
      name: 'GlueCatalogPermissions',
      tableWildcard: { },
    };

    return settings
  }

  private createLakeFormationServiceRole(policy: string) {
      const role = new iam.Role(this, "datalakeRole", {
        assumedBy: new iam.ServicePrincipal(policy),
        description: "Role used by lakeformation to access resources.",
        roleName: "LakeFormationServiceAccessRole",
        managedPolicies: [
          ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSGlueServiceRole"),
          ManagedPolicy.fromAwsManagedPolicyName("AWSLakeFormationDataAdmin"),
          ManagedPolicy.fromAwsManagedPolicyName("AWSGlueConsoleFullAccess"),
          ManagedPolicy.fromAwsManagedPolicyName("AmazonS3ReadOnlyAccess"),
        ]
      });
      role.addToPolicy(new iam.PolicyStatement(
          {
              effect:Effect.ALLOW,
              actions: ["iam:PassRole"],
              resources:[role.roleArn]
          }
      ))
      let resources_list = [];
      for (const element of this.Config.RegisteredBuckets) {
          const bucket_arn = element;
        resources_list.push(bucket_arn)
        resources_list.push(bucket_arn + "/*")
      }
      role.addToPolicy(new PolicyStatement({
          sid: 'LakeFormationDataAccessPermissionsForS3',
          actions: [
              "s3:PutObject",
              "s3:GetObject",
              "s3:DeleteObject",
              "s3:GetBucketLocation",
              "s3:GetObject",
              "s3:GetBucketAcl",
              "s3:GetObjectVersion",
              "s3:ListBucket",
          ],
          effect: Effect.ALLOW,
          resources: resources_list
      }))
      role.addToPolicy(new PolicyStatement({
          sid: 'LakeFormationDataAccessPermissionsForS3ListBuckets',
          actions: [
              "s3:ListAllMyBuckets"
          ],
          effect: Effect.ALLOW,
          resources: ["arn:aws:s3:::*"]
      }))

      role.addToPolicy(new PolicyStatement({
      sid: 'DataProducerExternalKeyAccess',
      actions: ['kms:Decrypt', 'kms:Encrypt', 'kms:GenerateDataKey'],
      effect: Effect.ALLOW,
      resources: [this.Config.BucketKeyArn]
    }));
    return role
  }

  private createGlueCrawlers(identifier: string, role_arn: string, dbName: string, buckets: string[]) {
    const crawlers: Record<string, string> = {}
      const cfnClassifier = new glueL2.CfnClassifier(this, 'csvCfnClassifier', /* all optional props */ {
          csvClassifier: {
              allowSingleColumn: false,
              containsHeader: 'PRESENT',
              delimiter: ',',
              disableValueTrimming: false,
              header: ['vendor','data_type','geography','contract_start','contract_end','primary_contact','prefix_directory','transform_job_name','statistical_analysis_job_name','data_classification','data_access_level'],
              name: 'csv_classifier',
          }
      });

     for (let i=0;i<buckets.length;i++) {
      const bucket_arn = buckets[i]
      const bucket = s3.Bucket.fromBucketArn(this,'bucket_' + i.toString(), bucket_arn)
      const name = identifier + "_" + i.toString()
      const crawler = new glueL2.CfnCrawler(this, name, {
          name: name,
          role: role_arn,
          databaseName: dbName,
          classifiers: ['csv_classifier'],
          targets: {
            s3Targets: [{ "path": "S3://" + bucket.bucketName}]
          },
          tags: {
           "bucket": bucket.bucketName
          },
      })
       if (crawler.name != null) {
         crawlers[bucket.bucketName] = crawler.name;
       }
     }

    return crawlers;
  }

  private createGlueServiceRole(identifier: string, servicePrincipal: string, policies: string[]) {
    const managedPolicies = [] as any;
    policies.forEach((p) => {
      managedPolicies.push(ManagedPolicy.fromAwsManagedPolicyName(p))
    });

    const service_role = new Role(this, identifier, {
      assumedBy: new ServicePrincipal(servicePrincipal),
      managedPolicies:managedPolicies,
    });

    service_role.addToPolicy(new PolicyStatement({
      sid: 'DataProducerExternalKeyAccess',
      actions: ['kms:Decrypt', 'kms:Encrypt', 'kms:GenerateDataKey'],
      effect: Effect.ALLOW,
      resources: [this.Config.BucketKeyArn]
    }));

    let resources_list = [];
    for (const bucket of this.Config.RegisteredBuckets) {
        const bucket_arn = bucket;
        resources_list.push(bucket_arn);
        resources_list.push(bucket_arn + "/*");
    }
    service_role.addToPolicy(new PolicyStatement({
      sid: 'DataProducerExternalBucketAccess',
      actions: [
          "s3:GetBucketLocation",
          "s3:GetObject",
          "s3:GetBucketAcl",
          "s3:GetObjectVersion",
          "s3:ListBucket",
          "s3:ListAllMyBuckets"
      ],
      effect: Effect.ALLOW,
      resources: resources_list
    }))

    return service_role
  }

  private createGlueDatabase() {
      return new glue.Database(this, this.Config.AppPrefix + '-Database', {
          databaseName: this.Config.AppPrefix + '-database'
      });
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

  private createLakeFormationCustomResourceRole() {
      return new iam.Role(this, 'MyCustomResourceLFRole', {
          assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
          managedPolicies: [
              iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaBasicExecutionRole"),
              iam.ManagedPolicy.fromAwsManagedPolicyName("AWSLakeFormationDataAdmin")
          ]
      })
  }

  private createLakeFormationTags(role:iam.IRole) {
    const onEvent = new lambda.Function(this, 'CustomLFTageLambda', {
      runtime: lambda.Runtime.PYTHON_3_9,
      code: lambda.Code.fromAsset(path.join(__dirname, '/../lambda'), {bundling: DataLakeStack.get_lambda_bundle()}),
      handler: "lake_formation_tags.on_event",
      tracing: lambda.Tracing.ACTIVE,
      reservedConcurrentExecutions: 5,
      environment: {
        REGION: cdk.Stack.of(this).region
      },
      role: role
    });

    const myProvider = new cr.Provider(this, 'MyProvider', {
      onEventHandler: onEvent,
      logRetention: logs.RetentionDays.ONE_DAY
    });

    new cdk.CustomResource(this, 'CustomLakeFormationTagsCreator', {
        serviceToken: myProvider.serviceToken,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
        resourceType:"Custom::LFTags",
        properties: { "tags" : this.Config.LakeFormationTags }
    });
  }

  private createCrawlersSecret(glueCrawlers:Record<string, string>, key: kms.IKey) {
      return new secrets_manager.CfnSecret(
          this, 'GlueCrawlersSecret', {
              name: 'DataLakeCrawlers',
              kmsKeyId: key.keyId,
              secretString: JSON.stringify({
                  glueCrawlers
              })
          }
      );
  }

   private createDataLakeAdminRole() {
       return new iam.Role(this, 'DataLakeAdmin', {
           assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
           managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName("AWSLakeFormationDataAdmin")]
       })
   }

   private datalakeRegisterS3buckets(){
       let resources_created = [];
       for (let i=0;i<this.Config.RegisteredBuckets.length; i++) {
           // create a resource location first
           const LF_resource = new lake_formation.CfnResource(this, 'DataLocationResource_' + i.toString(), {
               resourceArn:this.Config.RegisteredBuckets[i],
               roleArn:this.LakeFormationServiceRole.roleArn,
               useServiceLinkedRole: false
           });
           resources_created.push(LF_resource);
       }
       return resources_created;
   }

   private grantCrawlerRolePermissions(role: iam.IRole, gluedDB: glue.Database) {
        const dataLakePrincipal : lake_formation.CfnPermissions.DataLakePrincipalProperty = {
            dataLakePrincipalIdentifier: role.roleArn
        };

        // 1. Grant Glue Crawler permissions to lake formation database
       new lake_formation.CfnPermissions(this, 'GlueRoleDataLakePermissions_', {
           dataLakePrincipal: dataLakePrincipal,
           resource: {
               databaseResource: {
                   catalogId: gluedDB.catalogId,
                   name: gluedDB.databaseName,
               },
               tableResource: {
                   catalogId: gluedDB.catalogId,
                   databaseName: gluedDB.databaseName,
                   name: 'Glue Table Access',
                   tableWildcard: {},
               },
           },
           permissions: ['CREATE_TABLE', 'ALTER', 'DROP', 'DESCRIBE'],
       });

        // 2. iterate on all bucket and grant access to crawler for all bucket resources
        for (let i=0;i<this.Config.RegisteredBuckets.length; i++) {
            const dataLakeResource: lake_formation.CfnPermissions.ResourceProperty = {
                dataLocationResource: {
                    s3Resource: this.Config.RegisteredBuckets[i]
                }
            };
            const props:CfnPermissionsProps = {
                dataLakePrincipal: dataLakePrincipal,
                resource: dataLakeResource,
                permissions: ["DATA_LOCATION_ACCESS"]
            };
            const dataLocationPermission = new lake_formation.CfnPermissions(this, 'DataLocation_' + i.toString(), props)
            dataLocationPermission.addDependsOn(this.LakeFormationResources[i]);
        }
    }

   private createLakeFormationAnalystRole(identifier: string, roleName: string) {
    const role = new iam.Role(this, identifier, {
        assumedBy: new iam.AccountRootPrincipal(),
        roleName:roleName,
        managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonAthenaFullAccess"),
        iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonS3ReadOnlyAccess"),
        iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSGlueServiceRole")
        ]
    });
    role.addToPolicy(new iam.PolicyStatement({
           sid: 'AllowUseOfTheKey',
           actions: ['kms:Decrypt', 'kms:Encrypt', 'kms:ReEncrypt*', 'kms:GenerateDataKey*', "kms:DescribeKey"],
           effect: Effect.ALLOW,
           resources: [this.Config.BucketKeyArn]}));

    for (const element of this.Config.RegisteredBuckets) {
        let bucketArn = element;
         role.addToPolicy(new iam.PolicyStatement({
           sid: 'AllowUseOfTheBucket',
           actions: [
               "s3:GetBucketLocation",
               "s3:GetObject",
               "s3:ListBucket",
               "s3:ListBucketMultipartUploads",
               "s3:ListMultipartUploadParts"],
           effect: Effect.ALLOW,
           resources: [bucketArn, bucketArn +"/*"]}
         ))
    }
    return role;
  }

}