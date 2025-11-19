import * as cdk from 'aws-cdk-lib';
import {aws_ec2 as ec2} from 'aws-cdk-lib';
import {Construct} from 'constructs';

export interface EmrSecurityGroupsProps {
    /**
     * The VPC where security groups will be created
     */
    vpc: ec2.IVpc;

    /**
     * Prefix List ID for AWS Firewall Manager (com.amazonaws.firewall.regional-prod-only)
     */
    awsFirewallPrefixListId: string;
}

/**
 * Construct for creating EMR security groups with proper cross-referencing rules
 */
export class EmrSecurityGroups extends Construct {
    public readonly masterSecurityGroup: ec2.SecurityGroup;
    public readonly slaveSecurityGroup: ec2.SecurityGroup;
    private readonly props: EmrSecurityGroupsProps;

    constructor(scope: Construct, id: string, props: EmrSecurityGroupsProps) {
        super(scope, id);
        this.props = props;

        // EMR Master Security Group
        this.masterSecurityGroup = new ec2.SecurityGroup(this, 'MasterSecurityGroup', {
            vpc: this.props.vpc,
            description: 'Master security group for EMR cluster',
            allowAllOutbound: true
        });

        // EMR Slave Security Group
        this.slaveSecurityGroup = new ec2.SecurityGroup(this, 'SlaveSecurityGroup', {
            vpc: this.props.vpc,
            description: 'Slave security group for EMR cluster',
            allowAllOutbound: true
        });

        // Add EMR tags required by AWS EMR service
        cdk.Tags.of(this.masterSecurityGroup).add('for-use-with-amazon-emr-managed-policies', 'true');
        cdk.Tags.of(this.slaveSecurityGroup).add('for-use-with-amazon-emr-managed-policies', 'true');

        // Setup cross-referencing security group rules
        this.setupSecurityGroupRules();
    }

    /**
     * Setup security group rules that allow communication between master and slave groups
     */
    private setupSecurityGroupRules(): void {
        // Master Security Group Rules
        // Allow TCP traffic from itself and slave group
        this.masterSecurityGroup.addIngressRule(
            this.masterSecurityGroup,
            ec2.Port.tcpRange(0, 65535),
            'Allow TCP from master security group'
        );
        this.masterSecurityGroup.addIngressRule(
            this.slaveSecurityGroup,
            ec2.Port.tcpRange(0, 65535),
            'Allow TCP from slave security group'
        );

        // Allow UDP traffic from itself and slave group
        this.masterSecurityGroup.addIngressRule(
            this.masterSecurityGroup,
            ec2.Port.udpRange(0, 65535),
            'Allow UDP from master security group'
        );
        this.masterSecurityGroup.addIngressRule(
            this.slaveSecurityGroup,
            ec2.Port.udpRange(0, 65535),
            'Allow UDP from slave security group'
        );

        // Allow ICMP traffic from itself and slave group
        this.masterSecurityGroup.addIngressRule(
            this.masterSecurityGroup,
            ec2.Port.allIcmp(),
            'Allow ICMP from master security group'
        );
        this.masterSecurityGroup.addIngressRule(
            this.slaveSecurityGroup,
            ec2.Port.allIcmp(),
            'Allow ICMP from slave security group'
        );

        // Allow access from AWS services (EMR, etc.) via prefix list
        this.masterSecurityGroup.connections.allowFrom(
            ec2.Peer.prefixList(this.props.awsFirewallPrefixListId),
            ec2.Port.tcp(8443),
            'Allow EMR service access on port 8443'
        );

        // Slave Security Group Rules
        // Allow TCP traffic from itself and master group
        this.slaveSecurityGroup.addIngressRule(
            this.slaveSecurityGroup,
            ec2.Port.tcpRange(0, 65535),
            'Allow TCP from slave security group'
        );
        this.slaveSecurityGroup.addIngressRule(
            this.masterSecurityGroup,
            ec2.Port.tcpRange(0, 65535),
            'Allow TCP from master security group'
        );

        // Allow UDP traffic from itself and master group
        this.slaveSecurityGroup.addIngressRule(
            this.slaveSecurityGroup,
            ec2.Port.udpRange(0, 65535),
            'Allow UDP from slave security group'
        );
        this.slaveSecurityGroup.addIngressRule(
            this.masterSecurityGroup,
            ec2.Port.udpRange(0, 65535),
            'Allow UDP from master security group'
        );

        // Allow ICMP traffic from itself and master group
        this.slaveSecurityGroup.addIngressRule(
            this.slaveSecurityGroup,
            ec2.Port.allIcmp(),
            'Allow ICMP from slave security group'
        );
        this.slaveSecurityGroup.addIngressRule(
            this.masterSecurityGroup,
            ec2.Port.allIcmp(),
            'Allow ICMP from master security group'
        );
    }
}