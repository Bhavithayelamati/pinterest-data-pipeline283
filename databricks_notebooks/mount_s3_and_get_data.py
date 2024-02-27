# import pyspark functions
from pyspark.sql.functions import *
# import URL processing
import urllib

# Read the CSV file to spark dataframe, passing in options for the header row and separator
aws_keys_df = spark.read.format("csv")\
.option("header", "true")\
.option("sep", ",")\
.load("/FileStore/tables/authentication_credentials.csv")

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.where(col('User name')=='1273a52843e9').select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.where(col('User name')=='1273a52843e9').select('Secret access key').collect()[0]['Secret access key']
# Encode the secret key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# AWS S3 bucket name
AWS_S3_BUCKET = "user-1273a52843e9-bucket"
# Mount name for the bucket
MOUNT_NAME = "/mnt/user-1273a52843e9-bucket"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# list the topics stored on the mounted S3 bucket
display(dbutils.fs.ls("/mnt/user-1273a52843e9-bucket/topics"))

# list of topic suffixes
topics = [".pin", ".geo", ".user"]

def read_topics_into_dataframe(topic):
    # create path to topic files
    file_path = f"/mnt/user-1273a52843e9-bucket/topics/1273a52843e9{topic}/partition=0/*.json"
    # specify file type
    file_type = "json"
    # Ask Spark to infer the schema
    infer_schema = "true"
    # load JSONs from mounted S3 bucket to Spark dataframe
    df = spark.read.format(file_type) \
        .option("inferSchema", infer_schema) \
        .load(file_path)
    return df

for item in topics:
    # create statement strings for naming and displaying dataframes
    make_df_statement = f"df_{item[1:]} = read_topics_into_dataframe('{item}')"
    display_df_statement = f"display(df_{item[1:]})"
    # execute statements
    exec(make_df_statement)
    exec(display_df_statement)

# unmount the bucket from the filestore
dbutils.fs.unmount("/mnt/user-1273a52843e9-bucket")
