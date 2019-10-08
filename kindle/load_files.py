import boto3
from botocore.exceptions import NoCredentialsError
import configparser
from kindle.sql_queries import copy_table_queries, create_table_queries, drop_table_queries
import psycopg2
import pandas as pd
import csv
import os

config = configparser.ConfigParser()
config.read_file(open("kindle/dwh.cfg"))

KEY = config.get("AWS", "KEY")
SECRET = config.get("AWS", "SECRET")


def upload_to_aws(local_file, bucket, s3_file):
    """Uploads a local file to s3.
    """
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
    
def row_count_test(cur, con, csv, dbtable):
    """Compare csv rowcount with the loaded database table.
    """
    df = pd.read_csv(csv)
    csvrows = len(df) + 1
    cur.execute(f"SELECT COUNT(*) FROM {dbtable}")
    tablerows = cur.fetchone()
    if csvrows == tablerows[0]:
        print(f"Row counts match for {csv} and {dbtable}")
    else:
        print(f"Row counts don't match for {csv} and {dbtable}")

def main():
    config = configparser.ConfigParser()
    config.read_file(open("kindle/dwh.cfg"))

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
    print("Loading books to s3...")
    upload_to_aws('Data/processed/amazon_com_extras_processed.csv', 'kindle-reviews-and-sales', 'books.csv')

    print("Loading reviews to s3...")
    upload_to_aws('Data/processed/kindle_reviews_processed.csv', 'kindle-reviews-and-sales', 'reviews.csv')

    print("Loading salesranks to s3...")
    files = os.listdir('Data/processed/ranks_norm/')
    for file in files:
        upload_to_aws(f"Data/processed/ranks_norm/{file}", 'kindle-reviews-and-sales', f"ranks_norm/{file}")

    # load files from s3 to redshift
    con = psycopg2.connect(f"dbname={DWH_DB} host={DWH_ENDPOINT} port={DWH_PORT} user={DWH_DB_USER} password={DWH_DB_PASSWORD}")
    cur = con.cursor()

    drop_tables(cur, con)
    create_tables(cur, con)
    load_staging_tables(cur, con)

    # Data quality checks
    row_count_test(cur, con, 'Data/processed/amazon_com_extras_processed.csv', 'books')
    row_count_test(cur, con, 'Data/processed/kindle_reviews_processed.csv', 'reviews')

if __name__ == "__main__":
    main()