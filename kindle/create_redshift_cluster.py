import pandas as pd
import boto3
import json
import psycopg2
import configparser
import time

def create_cluster():
    """Create redshift cluster.
    """
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

    # Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)
    try:
        iam.create_role(RoleName=DWH_IAM_ROLE_NAME, Description="Allows Redshift clusters to call AWS services on your behalf.",
                        AssumeRolePolicyDocument=json.dumps(
                            {'Statement': [{'Action': 'sts:AssumeRole', 'Effect': 'Allow', 'Principal': {'Service': 'redshift.amazonaws.com'}}],
                                'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)

    print("1.2 Attaching Policy")

    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                        )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    print(roleArn)

    # Create Redshift Cluster

    try:
        response = redshift.create_cluster(        
            # HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            # Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            
            # Roles (for s3 access)
            IamRoles=[roleArn]  
        )
    except Exception as e:
        print(e)

    return redshift, DWH_CLUSTER_IDENTIFIER

def prettyRedshiftProps(props):
    """Get the status of the cluster.
    """
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


# When the cluster is available, this shows the clusters endpoint and role arn
def set_cluster_config():
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)

    # save endpoint and ARN to config file

    config['DWH']['DWH_ENDPOINT'] = DWH_ENDPOINT
    config['DWH']['DWH_ROLE_ARN'] = DWH_ROLE_ARN

    with open('kindle/dwh.cfg', 'w') as configfile:
        config.write(configfile)

    # Open incoming TCP port to access cluster endpoint

    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)


def main():

    redshift, DWH_CLUSTER_IDENTIFIER = create_cluster()
    myCluster = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    state = prettyRedshiftProps(myCluster).loc[[2]].Value.any()

    print(f'Launching Cluster. Cluster Status is {state}')

    while state != 'available':
        time.sleep(60)
        myCluster = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        state = prettyRedshiftProps(myCluster).loc[[2]].Value.any()
        print(f'Cluster Status is {state}')
    

if __name__ == "__main__":
    main()
