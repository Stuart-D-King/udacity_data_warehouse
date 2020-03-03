import configparser
import boto3
import json
import time
from botocore.exceptions import ClientError


def create_clients(key, secret):
    '''
    Using the Amazon Web Services (AWS) SDK for Python - boto3 - and the passed in AWS access key and secret key, create and return interfaces with EC2, S3, IAM, and Redshift

    INPUT
    key: AWS access key string
    secret: AWE secret key string

    OUTPUT
    ec2: AWS Elastic Compute Cloud (EC2) boto3 resource instance
    s3: AWS Simple Storage Service (S3) boto3 resource instance
    iam: AWS Identity and Access Management (IAM) boto3 client instance
    redshift: AWS Redshift boto3 client instance
    '''
    try:
        ec2 = boto3.resource('ec2',
            region_name='us-west-2',
            aws_access_key_id=key,
            aws_secret_access_key=secret)

        s3 = boto3.resource('s3',
            region_name='us-west-2',
            aws_access_key_id=key,
            aws_secret_access_key=secret)

        iam = boto3.client('iam',
            region_name='us-west-2',
            aws_access_key_id=key,
            aws_secret_access_key=secret)

        redshift = boto3.client('redshift',
            region_name='us-west-2',
            aws_access_key_id=key,
            aws_secret_access_key=secret)

        return ec2, s3, iam, redshift

    except Exception as e:
        print(e)


def create_iam_role(iam, iam_role_name):
    '''
    Create a new IAM role with AWS S3 read-only access using the passed in AWS SDK interface for IAM, and the passed in name for the new role

    INPUT
    iam: AWS IAM boto3 client instance
    iam_role_name: new IAM role name string

    OUTPUT
    roleArn: Amazon Resource Name
    '''
    try:
        dwhRole = iam.create_role(
            Path='/',
            RoleName=iam_role_name,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )

        iam.attach_role_policy(RoleName=iam_role_name,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']

        roleArn = iam.get_role(RoleName=iam_role_name)['Role']['Arn']

        return roleArn

    except Exception as e:
        print(e)


def create_redshift_cluster(redshift, cluster_type, node_type, num_nodes, name, cluster_identifier, user, password, arn):
    '''
    Create a new AWS Redshift cluster in Virtual Private Cloud (VPC)

    INPUT
    redshift: AWS Redshift boto3 client instance
    cluster_type: type of the cluster
    node_type: node type to be provisioned for the cluster
    num_nodes: number of compute nodes in the cluster
    name: name of the database to be created when the cluster is created
    cluster_identifier: unique identifier for the cluster - use this identifier to refer to the cluster for any subsequent cluster operations
    user: user name associated with the master user account
    password: password associated with the master user account
    arn: list of AWS Identity and Access Management (IAM) roles that can be used by the cluster to access other AWS services

    OUTPUT
    response: dictionary of newly created cluster parameters
    '''
    try:
        response = redshift.create_cluster(
            #HW
            ClusterType=cluster_type,
            NodeType=node_type,
            NumberOfNodes=int(num_nodes),

            #Identifiers & Credentials
            DBName=name,
            ClusterIdentifier=cluster_identifier,
            MasterUsername=user,
            MasterUserPassword=password,

            #Roles (for s3 access)
            IamRoles=[arn]
        )

        return response

    except Exception as e:
        print(e)


def access_endpoint(ec2, props, port):
    '''
    Open an incoming TCP port to access the cluster endpoint

    INPUT
    ec2: AWS EC2 boto3 resource instance
    props: dictionary of AWS Redshfit cluster properties
    port: port used for the TCP protocols

    OUTPUT
    none
    '''
    try:
        vpc = ec2.Vpc(id=props['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(port),
            ToPort=int(port)
        )
    except Exception as e:
        print(e)


def main():
    '''
    Create a new IAM role and use that newly created role to create a Redshift cluster. Print the IAM role ARN and cluster endpoint.
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    ec2, s3, iam, redshift = create_clients(config.get('AWS', 'KEY'), config.get('AWS', 'SECRET'))

    roleArn = create_iam_role(iam, config.get('CLUSTER', 'DWH_IAM_ROLE_NAME'))

    response = create_redshift_cluster(redshift,
        config.get('CLUSTER', 'DWH_CLUSTER_TYPE'),
        config.get('CLUSTER', 'DWH_NODE_TYPE'),
        config.get('CLUSTER', 'DWH_NUM_NODES'),
        config.get('CLUSTER', 'DWH_NAME'),
        config.get('CLUSTER', 'DWH_CLUSTER_IDENTIFIER'),
        config.get('CLUSTER', 'DWH_USER'),
        config.get('CLUSTER', 'DWH_PASSWORD'),
        roleArn
        )

    status = 'unavailalbe'
    print('Waiting for cluster to become available...')
    while status != 'available':
        time.sleep(5)
        status = redshift.describe_clusters(ClusterIdentifier=config.get('CLUSTER', 'DWH_CLUSTER_IDENTIFIER'))['Clusters'][0]['ClusterStatus']
    print('Cluster is available!')

    clusterProps = redshift.describe_clusters(ClusterIdentifier=config.get('CLUSTER', 'DWH_CLUSTER_IDENTIFIER'))['Clusters'][0]

    endpoint = clusterProps['Endpoint']['Address']

    access_endpoint(ec2, clusterProps, config.get('CLUSTER', 'DWH_PORT'))

    print('HOST :: ', endpoint)
    print('ARN :: ', roleArn)


if __name__ == "__main__":
    main()
