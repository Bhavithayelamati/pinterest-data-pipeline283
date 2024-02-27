# Clean df_pin dataframe
def add_nulls_to_dataframe_column(dataframe, column, value_to_replace):
    '''Converts matched values in column of dataframe to null based on expression'''
    dataframe = dataframe.withColumn(column, when(col(column).like(value_to_replace), None).otherwise(col(column)))
    return dataframe

# Replace empty entries and entries with no relevant data in each column with Nones
# Column names and values to change to null
columns_and_values_for_null = {
    "description": "No description available%",
    "follower_count": "User Info Error",
    "image_src": "Image src error.",
    "poster_name": "User Info Error",
    "tag_list": "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e",
    "title": "No Title Data Available"
}

# Loop through dictionary, calling function with dictionary values as arguments
for key, value in columns_and_values_for_null.items():
    df_pin = add_nulls_to_dataframe_column(df_pin, key, value)

# Perform the necessary transformations on the follower_count to ensure every entry is a number
df_pin = df_pin.withColumn("follower_count", regexp_replace("follower_count", "k", "000"))
df_pin = df_pin.withColumn("follower_count", regexp_replace("follower_count", "M", "000000"))
# Cast follower_count column to integer type
df_pin = df_pin.withColumn("follower_count", col("follower_count").cast('int'))
# Convert save_location column to include only the save location path
df_pin = df_pin.withColumn("save_location", regexp_replace("save_location", "Local save in ", ""))
# Rename the index column to ind
df_pin = df_pin.withColumnRenamed("index", "ind")
# Reorder columns
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

# Display changes
df_pin.printSchema()

# Clean df_geo dataframe
# Import types
from pyspark.sql.types import ArrayType, DoubleType
# Define function for returning list containing two values
def combine_lat_and_long(latitude, longitude):
    return [latitude, longitude]
# Define new user-defined function
new_func = udf(combine_lat_and_long, ArrayType(DoubleType()))
# Apply new udf to combine latitude and longitude columns
df_geo = df_geo.withColumn("coordinates", new_func("latitude", "longitude"))
# Drop the latitude and longitude columns
cols_to_drop = ("latitude", "longitude")
df_geo = df_geo.drop(*cols_to_drop)
# Convert timestamp column from type string to type timestamp
df_geo = df_geo.withColumn("timestamp", to_timestamp("timestamp"))
# Change column order
new_geo_column_order = [
    "ind",
    "country",
    "coordinates",
    "timestamp",
]
df_geo = df_geo.select(new_geo_column_order)

# Display changes
df_geo.printSchema()

# Clean df_user dataframe
# Create new column for full name
df_user = df_user.withColumn("user_name", concat_ws(" ", "first_name", "last_name"))
# Drop the first_name and last_name columns
cols_to_drop = ("first_name", "last_name")
df_user = df_user.drop(*cols_to_drop)
# Convert date_joined column from type string to type timestamp
df_user = df_user.withColumn("date_joined", to_timestamp("date_joined"))
# Change column order
new_user_column_order = [
    "ind",
    "user_name",
    "age",
    "date_joined",
]
df_user = df_user.select(new_user_column_order)

# Display changes
df_user.printSchema()
