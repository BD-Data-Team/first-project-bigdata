#!/usr/bin/env python3
"""spark application"""


import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, LongType


parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")


# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# initialize SparkSession
# with the proper configuration
spark = SparkSession \
    .builder \
    .appName("Fist task") \
    .getOrCreate()

custom_schema = StructType([
    StructField(name="id", dataType=IntegerType(), nullable=True),
    StructField(name="product_id", dataType=StringType(), nullable=True),
    StructField(name="user_id", dataType=StringType(), nullable=True),
    StructField(name="profile_name", dataType=StringType(), nullable=True),
    StructField(name="helpfulness_numerator",
                dataType=IntegerType(), nullable=True),
    StructField(name="helpfulness_denominator",
                dataType=IntegerType(), nullable=True),
    StructField(name="score", dataType=IntegerType(), nullable=True),
    StructField(name="time", dataType=LongType(), nullable=True),
    StructField(name="summary", dataType=StringType(), nullable=True),
    StructField(name="text", dataType=StringType(), nullable=True)])

input_DF = spark.read.csv(
    input_filepath, schema=custom_schema, header=True).cache()

input_DF.createOrReplaceTempView("reviews")

reviews_per_year_DF = spark.sql(
    "SELECT year(from_unixtime(time)) as reviews_year, product_id, COUNT(*) as reviews_count FROM reviews GROUP BY YEAR(from_unixtime(time)), product_id;")

reviews_per_year_DF.createOrReplaceTempView("reviews_per_year")

top_10_products_for_year_DF = spark.sql(
    "SELECT reviews_year, product_id FROM ( SELECT *, row_number() OVER (PARTITION BY reviews_year ORDER BY reviews_count DESC) as row_num FROM reviews_per_year  ) as ranked_reviews_per_year WHERE row_num <= 10;")

top_10_products_for_year_DF.createOrReplaceTempView("top_10_products_for_year")

top_10_products_for_year_with_reviews_DF = spark.sql(
    "SELECT top_10_products_for_year.reviews_year as reviews_year, top_10_products_for_year.product_id as product_id, reviews.text as text FROM top_10_products_for_year, reviews WHERE top_10_products_for_year.reviews_year = YEAR(from_unixtime(time)) AND top_10_products_for_year.product_id = reviews.product_id;")

top_10_products_for_year_with_reviews_DF.createOrReplaceTempView(
    "top_10_products_for_year_with_reviews")

year_for_product_2_word_count_DF = spark.sql(
    "SELECT reviews_year, product_id, exploded_text.word as word, COUNT(*) as word_count FROM top_10_products_for_year_with_reviews  LATERAL VIEW explode(split(text, ' ')) exploded_text AS word WHERE length(exploded_text.word) >= 4 GROUP BY reviews_year, product_id, exploded_text.word;")

year_for_product_2_word_count_DF.createOrReplaceTempView(
    "year_for_product_2_word_count")

output_DF = spark.sql("SELECT reviews_year, product_id, word, word_count FROM (  SELECT *, row_number() OVER (PARTITION BY reviews_year, product_id ORDER BY word_count DESC) as row_num FROM year_for_product_2_word_count  ) as ranked_year_for_product_2_word_count WHERE row_num <= 5;")

output_DF.show()

# output_DF.saveAsTextFile(output_filepath)
