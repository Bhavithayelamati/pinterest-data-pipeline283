# import for performing window functions
from pyspark.sql.window import Window
# join df_pin and df_geo dataframes on index
pin_geo = df_pin.join(df_geo, df_pin.ind == df_geo.ind)
# join df_pin and df_user and create temp view for SQL query
df_pin.join(df_user, df_pin.ind == df_user.ind).createOrReplaceTempView("category_age")
# SQL query to create age group column
pin_user_age_group = spark.sql(
    "SELECT CASE \
        WHEN age between 18 and 24 then '18-24' \
        WHEN age between 25 and 35 then '25-35' \
        WHEN age between 36 and 50 then '36-50' \
        WHEN age > 50 then '50+' \
        END as age_group, * FROM category_age")
# Find the most popular category in each country
# create partition by country and order by category_count descending
windowCountryByCatCount = Window.partitionBy("country").orderBy(col("category_count").desc())
# find the most popular category in each country
pin_geo.groupBy("country", "category") \
.agg(count("category") \
.alias("category_count")) \
.withColumn("rank", row_number().over(windowCountryByCatCount)) \
.filter(col("rank") == 1) \
.drop("rank") \
.show()
# Find the most popular category each year
# create partition by year and order by category_count descending
windowYearByCatCount = Window.partitionBy("post_year").orderBy(col("category_count").desc())
# find which was the most popular category each year between 2018 and 2022
pin_geo.withColumn("post_year", year("timestamp")) \
.filter(col("post_year") >= 2018) \
.filter(col("post_year") <= 2022) \
.groupBy("post_year", "category") \
.agg(count("category").alias("category_count")) \
.withColumn("rank", row_number().over(windowYearByCatCount)) \
.filter(col("rank") == 1) \
.drop("rank") \
.show()
# Find the user with the most followers in each country
# create partition by country and order by follower_count descending
windowCountryByFollowers = Window.partitionBy("country").orderBy(col("follower_count").desc())

# find the user with the most followers in each country
max_followers_by_country = \
    df_pin.join(df_geo, df_pin.ind == df_geo.ind) \
    .withColumn("rank", row_number().over(windowCountryByFollowers)) \
    .filter(col("rank") == 1) \
    .select("country", "poster_name", "follower_count")

# get highest number of followers from all countries
max_followers_all_countries = max_followers_by_country.select(max("follower_count")).collect()[0][0]

# find the country with the user with most followers
country_with_max_followers = \
    max_followers_by_country \
    .select("*") \
    .where(col("follower_count") == max_followers_all_countries)

max_followers_by_country.show()
country_with_max_followers.show()
# Find the most popular category for different age groups
# create partition by age_group and order by category_count descending
windowAgeGroup = Window.partitionBy("age_group").orderBy(col("category_count").desc())
# find the most popular category for different age groups
pin_user_age_group.groupBy("age_group", "category") \
.agg(count("category").alias("category_count")) \
.withColumn("rank", row_number().over(windowAgeGroup)) \
.filter(col("rank") == 1) \
.drop("rank") \
.show()
# Find the median follower count for different age groups
# find the median follower count for different age groups
pin_user_age_group \
.select("user_name", "date_joined", "age_group", "follower_count") \
.distinct() \
.groupBy("age_group") \
.agg(percentile_approx("follower_count", 0.5).alias("median_follower_count")) \
.orderBy("age_group") \
.show()
# Find out how many users joined each year
# find out how many users joined each year
df_user.withColumn("post_year", year("date_joined")) \
.drop("ind") \
.distinct() \
.groupBy("post_year") \
.agg(count("user_name").alias("number_users_joined")) \
.orderBy("post_year") \
.show()
# Find the median follower count of users based on their joining year
# find the median follower count of users based on their joining year
pin_user_age_group \
.select("user_name", "date_joined", "follower_count") \
.distinct() \
.withColumn("post_year", year("date_joined")) \
.groupBy("post_year") \
.agg(percentile_approx("follower_count", 0.5).alias("median_follower_count")) \
.orderBy("post_year") \
.show()
# Find the median follower count of users that have joined between 2015 and 2020, based on which age group they are part of
# find the median follower count of users that have joined between 2015 and 2020, based on which age group they are part of
pin_user_age_group \
.select("user_name", "age_group", "date_joined", "follower_count") \
.distinct() \
.withColumn("post_year", year("date_joined")) \
.groupBy("post_year", "age_group") \
.agg(percentile_approx("follower_count", 0.5).alias("median_follower_count")) \
.orderBy("post_year", "age_group") \
.show()
