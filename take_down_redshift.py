import configparser
import boto3
import os
from launch_redshift import create_clients


def aws_cleanup(redshift, iam, cluster_identifier, iam_role_name):
    '''
    Delete a Redshift cluster and the IAM role associated with the cluster

    INPUT
    redshift: AWS Redshift boto3 client instance
    iam: AWS IAM boto3 client instance
    cluster_identifier: unique cluster identifier
    iam_role_name: AWS IAM role name string

    OUTPUT
    None
    '''
    try:
        # deleted Redshift cluster
        redshift.delete_cluster(ClusterIdentifier=cluster_identifier, SkipFinalClusterSnapshot=True)
        print('Redshift cluster deleted')
    except Exception as e:
        print(e)

    try:
        # delete IAM role
        iam.detach_role_policy(RoleName=iam_role_name, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=iam_role_name)
        print('IAM role detached and deleted')
    except Exception as e:
        print(e)


def main():
    '''
    Delete the AWS Redshift cluster and IAM role
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    ec2, s3, iam, redshift = create_clients(os.environ.get('AWS_ACCESS_KEY_DWHADMIN'), os.environ.get('AWS_SECRET_ACCESS_KEY_DWHADMIN'))

    aws_cleanup(redshift, iam,
        config.get('CLUSTER', 'DWH_CLUSTER_IDENTIFIER'),
        config.get('CLUSTER', 'DWH_IAM_ROLE_NAME'))


if __name__ == '__main__':
    main()
