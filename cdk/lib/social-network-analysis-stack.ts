import * as cdk from 'aws-cdk-lib';
import {Construct} from 'constructs';
import {aws_s3 as s3} from 'aws-cdk-lib';
import {aws_s3_deployment as s3deploy} from 'aws-cdk-lib';
import { aws_ec2 as ec2 } from 'aws-cdk-lib';
import {aws_emr as emr} from 'aws-cdk-lib';
import {aws_iam as iam} from 'aws-cdk-lib';
import {EmrSecurityGroups} from './emr-security-groups';

export class SocialNetworkAnalysisStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props?: cdk.StackProps) {
        super(scope, id, props);

        // ========================================
        // Get context parameters and import existing resources
        // ========================================
        const vpcId = this.node.tryGetContext("vpcId");
        if (!vpcId) {
            throw new Error("VPC ID must be provided in context with key 'vpc'");
        }
        const vpc = ec2.Vpc.fromLookup(this, 'ExistingVpc', {
            vpcId: String(vpcId)
        });

        const awsFirewallPrefixListId = this.node.tryGetContext("awsFirewallPrefixListId");
        if (!awsFirewallPrefixListId) {
            throw new Error("AWS Firewall Prefix List ID must be provided in context with key 'awsFirewallPrefixListId'");
        }

        const ec2KeyPairName = this.node.tryGetContext("ec2KeyPairName");
        if (!ec2KeyPairName) {
            throw new Error("EC2 Key Pair Name must be provided in context with key 'ec2KeyPairName'");
        }
        const ec2KeyPair = ec2.KeyPair.fromKeyPairName(this, 'ImportedKeyPair', String(ec2KeyPairName));

        const subnetId = this.node.tryGetContext("subnetId");
        if (!subnetId) {
            throw new Error("Subnet ID must be provided in context with key 'subnetId'");
        }
        const subnet = ec2.Subnet.fromSubnetId(this, "ExistingSubnet", subnetId);
        cdk.Annotations.of(subnet).acknowledgeWarning(
            "@aws-cdk/aws-ec2:noSubnetRouteTableId",
            "Will not read route table ID for subnet",
        );

        // ========================================
        // S3 Bucket for Social Network Analysis
        // ========================================
        const contextBucketName = this.node.tryGetContext("s3BucketName");
        const bucketProps: s3.BucketProps = {
            // Use context if provided, otherwise let CDK auto-generate globally unique name
            ...(contextBucketName && { bucketName: contextBucketName }),
            encryption: s3.BucketEncryption.S3_MANAGED,
            versioned: false,
            removalPolicy: cdk.RemovalPolicy.RETAIN,
            autoDeleteObjects: false,
            blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
            lifecycleRules: [
                {
                    id: 'DeleteIncompleteMultipartUploads',
                    abortIncompleteMultipartUploadAfter: cdk.Duration.days(7)
                }
            ]
        };
        const bucket = new s3.Bucket(this, 'SocialNetworkAnalysisBucket', bucketProps);

        // Create folder structure by deploying empty marker files
        new s3deploy.BucketDeployment(this, 'CreateBucketFolders', {
            sources: [
                s3deploy.Source.data('data/.keep', ' '),
                s3deploy.Source.data('jars/.keep', ' '),
                s3deploy.Source.data('logs/.keep', ' '),
                s3deploy.Source.data('output/.keep', ' '),
            ],
            destinationBucket: bucket,
        });

        // ========================================
        // IAM Roles for EMR
        // ========================================
        const emrServiceRole = new iam.Role(this, 'EmrServiceRole', {
            assumedBy: new iam.ServicePrincipal('elasticmapreduce.amazonaws.com'),
            managedPolicies: [
                iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AmazonElasticMapReduceRole')
            ],
            description: 'Service role for EMR cluster management'
        });

        const emrEc2Role = new iam.Role(this, 'EmrEc2InstanceRole', {
            assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
            managedPolicies: [
                iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AmazonElasticMapReduceforEC2Role')
            ],
            description: 'Instance profile role for EMR EC2 instances'
        });

        const emrEc2InstanceProfile = new iam.CfnInstanceProfile(this, 'EmrEc2InstanceProfile', {
            roles: [emrEc2Role.roleName]
            // Let CDK auto-generate the InstanceProfile name to avoid conflicts
        });

        const emrAutoScalingRole = new iam.Role(this, 'EmrAutoScalingRole', {
            assumedBy: new iam.CompositePrincipal(
                new iam.ServicePrincipal('elasticmapreduce.amazonaws.com'),
                new iam.ServicePrincipal('application-autoscaling.amazonaws.com')
            ),
            managedPolicies: [
                iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AmazonElasticMapReduceforAutoScalingRole')
            ],
            description: 'Role for EMR automatic scaling functionality'
        });

        // ========================================
        // Security Groups for EMR
        // ========================================
        const emrSecurityGroups = new EmrSecurityGroups(this, 'EmrSecurityGroups', {
            vpc: vpc,
            awsFirewallPrefixListId: String(awsFirewallPrefixListId)
        });


        // ========================================
        // EMR Cluster Configuration
        // ========================================
        const cluster = new emr.CfnCluster(this, 'SocialNetworkAnalysisCluster', {
            name: 'social-network-analysis-cluster',
            releaseLabel: 'emr-7.11.0',

            applications: [
                {name: 'Hadoop'},
                {name: 'Spark'},
            ],

            instances: {
                ec2KeyName: ec2KeyPair.keyPairName,
                ec2SubnetId: subnet.subnetId,

                emrManagedMasterSecurityGroup: emrSecurityGroups.masterSecurityGroup.securityGroupId,
                emrManagedSlaveSecurityGroup: emrSecurityGroups.slaveSecurityGroup.securityGroupId,

                masterInstanceGroup: {
                    instanceCount: 1,
                    instanceType: 'm5.xlarge',
                    market: 'ON_DEMAND',
                    name: 'Primary',
                    ebsConfiguration: {
                        ebsBlockDeviceConfigs: [
                            {
                                volumesPerInstance: 2,
                                volumeSpecification: {
                                    volumeType: 'gp2',
                                    sizeInGb: 32,
                                },
                            },
                        ],
                    },
                },

                coreInstanceGroup: {
                    instanceCount: 5,
                    instanceType: 'm5.xlarge',
                    market: 'ON_DEMAND',
                    name: 'Core',
                    ebsConfiguration: {
                        ebsBlockDeviceConfigs: [
                            {
                                volumesPerInstance: 2,
                                volumeSpecification: {
                                    volumeType: 'gp2',
                                    sizeInGb: 32,
                                },
                            },
                        ],
                    },
                },
            },

            serviceRole: emrServiceRole.roleName,
            jobFlowRole: emrEc2InstanceProfile.ref,
            autoScalingRole: emrAutoScalingRole.roleName,

            logUri: `s3n://${bucket.bucketName}/logs/`,

            visibleToAllUsers: true,

            ebsRootVolumeSize: 20,
        });

        // ========================================
        // Stack Outputs
        // ========================================
        new cdk.CfnOutput(this, 'BucketName', {
            value: bucket.bucketName,
            description: 'S3 Bucket for social network analysis data',
            exportName: 'BucketName',
        });

        new cdk.CfnOutput(this, 'ClusterId', {
            value: cluster.ref,
            description: 'EMR Cluster ID',
            exportName: 'ClusterId',
        });
    }
}