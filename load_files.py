import boto3
from botocore.exceptions import NoCredentialsError
import configparser
from sql_queries import copy_table_queries, create_table_queries, drop_table_queries
import psycopg2
import pandas as pd
import csv

config = configparser.ConfigParser()
config.read_file(open("dwh.cfg"))

KEY = config.get("AWS", "KEY")
SECRET = config.get("AWS", "SECRET")


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

def drop_tables(cur, conn):
    """Drops database tables in Redshift if they exist.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """Creates database tables in Redshift.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


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

    # load files to s3
    upload_to_aws('Data/amazon-sales-rank-data-for-print-and-kindle-books/amazon_com_extras_processed.csv', 'kindle-reviews-and-sales', 'books.csv')
    upload_to_aws('Data/kindle-reviews/kindle_reviews.csv', 'kindle-reviews-and-sales', 'reviews.csv')

    files = os.listdir('Data/ranks_norm/processed')
    for file in files:
        upload_to_aws(file, 'kindle-reviews-and-sales', file)

    # load files from s3 to redshift
    con = psycopg2.connect(f"dbname={DWH_DB} host={DWH_ENDPOINT} port={DWH_PORT} user={DWH_DB_USER} password={DWH_DB_PASSWORD}")
    cur = con.cursor()

    drop_tables(cur, con)
    create_tables(cur, con)
    load_staging_tables(cur, con)

if __name__ == "__main__":
    main()