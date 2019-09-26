import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('kindle/dwh.cfg')

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
REVIEW_DATA = config.get("S3", "REVIEW_DATA")
SALESRANK_PATH = config.get("S3", "SALESRANK_PATH")

staging_books_table_drop = "DROP TABLE IF EXISTS public.books"
staging_books_reviews_drop = "DROP TABLE IF EXISTS public.reviews"
time_table_drop = "DROP TABLE IF EXISTS public.time"
salesrank_table_drop = "DROP TABLE IF EXISTS public.salesrank"

staging_books_table_create = ("""
CREATE TABLE public.books (
	asin varchar(256) NOT NULL,
	"group" varchar(50),
	format varchar(50),
	title varchar(500),
	author varchar(256),
  	publisher varchar(96))
""")

staging_reviews_table_create = ("""
CREATE TABLE public.reviews (
	asin varchar(256) NOT NULL,
	reviewerid varchar(50),
	reviewText varchar(4000),
	helpful varchar(500),
	summary varchar(256),
  	overall varchar(96),
	unixReviewTime varchar(96),
	reviewerName varchar(255),
	reviewTime varchar (50))
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time datetime PRIMARY KEY
                                ,hour int
                                ,day int
                                ,week int
                                ,month int
                                ,year int
                                ,weekday int);
""")

salesrank_table_create = ("""
CREATE TABLE IF NOT EXISTS salesrank (timestamp varchar(256)
								,rank varchar(256)
								,asin varchar(256));
""")

staging_books_copy = (f"""
COPY public.books
FROM {BOOK_DATA}
credentials 'aws_iam_role={DWH_ROLE_ARN}'
region 'us-west-2'
FORMAT AS CSV
""")

staging_reviews_copy = (f"""
COPY public.reviews
FROM {REVIEW_DATA}
credentials 'aws_iam_role={DWH_ROLE_ARN}'
region 'us-west-2'
FORMAT AS CSV
TRUNCATECOLUMNS
""")

salesrank_copy = (f"""
COPY public.salesrank
FROM {SALESRANK_PATH}
credentials 'aws_iam_role={DWH_ROLE_ARN}'
region 'us-west-2'
FORMAT AS CSV
""")

drop_table_queries = [staging_books_table_drop, staging_books_reviews_drop, time_table_drop, salesrank_table_drop]
create_table_queries = [staging_books_table_create, staging_reviews_table_create, time_table_create, salesrank_table_create]
copy_table_queries = [salesrank_copy, staging_books_copy, staging_reviews_copy] 