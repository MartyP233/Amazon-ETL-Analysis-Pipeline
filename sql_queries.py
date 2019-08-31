import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

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
DWH_ENDPOINT = config.get("DWH", "DWH_ENDPOINT")
DWH_ROLE_ARN = config.get("DWH", "DWH_ROLE_ARN")

LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
BOOK_DATA = config.get("S3", "BOOK_DATA")

staging_books_table_drop = "DROP TABLE IF EXISTS public.books"

staging_books_table_create = ("""
CREATE TABLE public.books (
	asin varchar(256) NOT NULL,
	"group" varchar(50),
	format varchar(50),
	title varchar(213),
	author varchar(256),
  	publisher varchar(96)
""")

staging_books_copy = (f"""
COPY public.books
FROM {BOOK_DATA}
credentials 'aws_iam_role={DWH_ROLE_ARN}'
region 'us-west-2'
FORMAT AS CSV
""")

copy_table_queries = [staging_books_copy,]