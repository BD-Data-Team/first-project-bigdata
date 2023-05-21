#!/usr/bin/env python3
"""spark application"""


import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, LongType
from pyspark.sql.functions import row_number, explode, split
from pyspark.sql import Window


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
    .config("spark.driver.host", "localhost") \
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
    StructField(name="review_year", dataType=IntegerType(), nullable=True),
    StructField(name="summary", dataType=StringType(), nullable=True),
    StructField(name="text", dataType=StringType(), nullable=True)])

input_DF = spark.read.csv(
    input_filepath, schema=custom_schema, header=True).cache()

reviews_per_year_DF = input_DF.groupBy(
    "reviews_year", "product_id").count().withColumnRenamed("count", "reviews_count")

win_temp = Window.partitionBy("reviews_year").orderBy(
    reviews_per_year_DF["reviews_count"].desc())

top_10_products_for_year_DF = reviews_per_year_DF.withColumn(
    "row_num", row_number().over(win_temp)).filter("row_num <= 10").drop("row_num").drop("reviews_count")

join_condition = [top_10_products_for_year_DF.reviews_year == input_DF.reviews_year,
                  top_10_products_for_year_DF.product_id == input_DF.product_id]

# drop necessary otherwise ambiguity column name error
top_10_products_for_year_with_reviews_DF = top_10_products_for_year_DF.join(input_DF, join_condition).drop(
    input_DF.product_id, input_DF.reviews_year).select("reviews_year", "product_id", "text")

top_10_products_for_year_with_reviews_DF = top_10_products_for_year_with_reviews_DF.withColumn(
    "word", explode(split(top_10_products_for_year_with_reviews_DF.text, " ")))

year_for_product_2_word_count_DF = top_10_products_for_year_with_reviews_DF.groupBy(
    "reviews_year", "product_id", "word").count().withColumnRenamed("count", "word_count")\
    .where("length(word) >= 4").select("reviews_year", "product_id", "word", "word_count")

win_temp2 = Window.partitionBy("reviews_year", "product_id").orderBy(
    year_for_product_2_word_count_DF["word_count"].desc())

output_DF = year_for_product_2_word_count_DF.withColumn(
    "row_num", row_number().over(win_temp2)).filter("row_num <= 5").drop("row_num").drop("reviews_count")

output_DF.write.csv(output_filepath, header=True)
