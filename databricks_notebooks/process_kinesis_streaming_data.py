# import pyspark functions
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, FloatType
# import URL processing
import urllib

# Read the CSV file to spark dataframe, passing in options for the header row and separator
aws_keys_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("sep", ",") \
    .load("/FileStore/tables/authentication_credentials.csv")

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.where(col('User name')=='1273a52843e9').select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.where(col('User name')=='1273a52843e9').select('Secret access key').collect()[0]['Secret access key']
# Encode the secret key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# define schemas for each of the dataframes
pin_schema = StructType([
    StructField("index", IntegerType()),
    StructField("unique_id", StringType()),
    StructField("title", StringType()),
    StructField("description", StringType()),
    StructField("poster_name", StringType()),
    StructField("follower_count", StringType()),
    StructField("tag_list", StringType()),
    StructField("is_image_or_video", StringType()),
    StructField("image_src", StringType()),
    StructField("downloaded", IntegerType()),
    StructField("save_location", StringType()),
    StructField("category", StringType())
])
geo_schema = StructType([
    StructField("ind", IntegerType()),
    StructField("timestamp", TimestampType()),
    StructField("latitude", FloatType()),
    StructField("longitude", FloatType()),
    StructField("country", StringType())
])
user_schema = StructType([
    StructField("ind", IntegerType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("age", StringType()),
    StructField("date_joined", TimestampType())
])

# define function to retrieve Kinesis stream and return it as dataframe
def get_stream(stream_name: str):
    dataframe = spark \
        .readStream \
        .format('kinesis') \
        .option('streamName', stream_name) \
        .option('initialPosition','earliest') \
        .option('region','us-east-1') \
        .option('awsAccessKey', ACCESS_KEY) \
        .option('awsSecretKey', SECRET_KEY) \
        .load()
    return dataframe

# define function to deserialize data from stream and return it as dataframe
def deserialize_stream(stream, schema):
    dataframe = stream \
        .selectExpr("CAST(data as STRING)") \
        .withColumn("data", from_json(col("data"), schema)) \
        .select(col("data.*"))
    return dataframe

# define function to convert matched values in column of dataframe to null based on expression
def add_nulls_to_dataframe_column(dataframe, column, value_to_replace):
    dataframe = dataframe.withColumn(column, when(col(column).like(value_to_replace), None).otherwise(col(column)))
    return dataframe

# define function to write dataframe to Delta table
def write_stream_df_to_table(dataframe, name: str):
    dataframe.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"/tmp/kinesis/1273a52843e9_{name}_table_checkpoints/") \
        .table(f"1273a52843e9_{name}_table")

# Get streams from Kinesis
pin_stream = get_stream('streaming-1273a52843e9-pin')
geo_stream = get_stream('streaming-1273a52843e9-geo')
user_stream = get_stream('streaming-1273a52843e9-user')

# Deserialize streams
df_pin = deserialize_stream(pin_stream, pin_schema)
df_geo = deserialize_stream(geo_stream, geo_schema)
df_user = deserialize_stream(user_stream, user_schema)

# Clean data
columns_and_values_for_null = {
    "description": "No description available%",
    "follower_count": "User Info Error",
    "image_src": "Image src error.",
    "poster_name": "User Info Error",
    "tag_list": "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e",
    "title": "No Title Data Available"
}

for key, value in columns_and_values_for_null.items():
    df_pin = add_nulls_to_dataframe_column(df_pin, key, value)

df_pin = df_pin.withColumn("follower_count", regexp_replace("follower_count", "k", "000"))
df_pin = df_pin.withColumn("follower_count", regexp_replace("follower_count", "M", "000000"))
df_pin = df_pin.withColumn("follower_count", col("follower_count").cast('int'))
df_pin = df_pin.withColumn("save_location", regexp_replace("save_location", "Local save in ", ""))
df_pin = df_pin.withColumnRenamed("index", "ind")
new_pin_column_order = [
    "ind",
    "unique_id",
    "title",
    "description",
    "follower_count",
    "poster_name",
    "tag_list",
    "is_image_or_video",
    "image_src",
    "save_location",
    "category"
]
df_pin = df_pin.select(new_pin_column_order)

# Define a user-defined function to combine latitude and longitude columns
def combine_lat_and_long(latitude, longitude):
    return [latitude, longitude]

# Apply the new udf to combine latitude and longitude columns
new_func = udf(combine_lat_and_long, ArrayType(DoubleType()))
df_geo = df_geo.withColumn("coordinates", new_func("latitude", "longitude"))

# Drop the latitude and longitude columns
cols_to_drop = ("latitude", "longitude")
df_geo = df_geo.drop(*cols_to_drop)

# Convert timestamp column from string to timestamp type
df_geo = df_geo.withColumn("timestamp", to_timestamp("timestamp"))
new_geo_column_order = [
    "ind",
    "country",
    "coordinates",
    "timestamp",
]
df_geo = df_geo.select(new_geo_column_order)

# Create a new column for full name
df_user = df_user.withColumn("user_name", concat_ws(" ", "first_name", "last_name"))
cols_to_drop = ("first_name", "last_name")
df_user = df_user.drop(*cols_to_drop)
df_user = df_user.withColumn
