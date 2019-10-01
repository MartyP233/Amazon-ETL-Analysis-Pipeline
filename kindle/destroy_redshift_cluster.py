import pandas as pd
import boto3
import json
import psycopg2
import configparser
import time


def prettyRedshiftProps(props):
    """Get the status of the cluster.
    """
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


# Clean up resources
def remove_cluster():

    # import env vars
    config = configparser.ConfigParser()
    config.read_file(open("kindle/dwh.cfg"))

    KEY = config.get("AWS", "KEY")
    SECRET = config.get("AWS", "SECRET")
    DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
    DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
    DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
    DWH_DB = config.get("DWH", "DWH_DB")
    DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
    DWH_PORT = config.get("DWH", "DWH_PORT")
    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

    # Create clients for IAM, EC2, S3 and Redshift

    ec2 = boto3.resource(
        "ec2", aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name="us-west-2"
    )

    s3 = boto3.resource(
        "s3", aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name="us-west-2"
    )

    iam = boto3.client(
        "iam", aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name="us-west-2"
    )

    redshift = boto3.client(
        "redshift", aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name="us-west-2"
    )

    # Delete Cluster

    redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)

    # Get status

    myCluster = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    # Delete roles

    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)

    return myCluster

def main():

    myCluster = remove_cluster()

    state = prettyRedshiftProps(myCluster).loc[[2]].Value.any()
    
    print(f'Cluster Status is {state}') 

if __name__ == "__main__":
    main()
