import boto3
from botocore.exceptions import NoCredentialsError
import configparser
from sql_queries import copy_table_queries
import psycopg2

def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=KEY,
                      aws_secret_access_key=SECRET)

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False

def load_staging_tables(cur, conn):
    """Loads data from S3 JSON files into redshift staging tables.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read_file(open("dwh.cfg"))

    KEY = config.get("AWS", "KEY")
    SECRET = config.get("AWS", "SECRET")
    DWH_DB = config.get("DWH", "DWH_DB")
    DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
    DWH_PORT = config.get("DWH", "DWH_PORT")
    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
    DWH_ENDPOINT = config.get("DWH", "DWH_ENDPOINT")
    DWH_ROLE_ARN = config.get("DWH", "DWH_ROLE_ARN")

    uploaded = upload_to_aws('Data/amazon-sales-rank-data-for-print-and-kindle-books/amazon_com_extras.csv', 'kindle-reviews-and-sales', 'books.csv')

    con = psycopg2.connect(f"dbname={DWH_DB} host={DWH_ENDPOINT} port={DWH_PORT} user={DWH_DB_USER} password={DWH_DB_PASSWORD}")
    cur = con.cursor()

    load_staging_tables(cur, con)

if __name__ == "__main__":
    main()