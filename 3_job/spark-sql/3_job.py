#!/usr/bin/env python3
"""spark application"""

import argparse
# create parser and set its arguments
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import collect_set, size, array, array_intersect, array_union


parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# initialize SparkSession
# with the proper configuration
spark = SparkSession \
    .builder \
    .config("spark.executor.memory", "5g") \
    .appName("Tird task") \
    .getOrCreate()

# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

input_df = spark.read.csv(input_filepath, header=True,
                          inferSchema=True).cache()

productId_for_users_df = input_df.select("UserId", "ProductId").where(input_df["Score"] >= 4) \
    .groupBy("ProductId").agg(collect_set("UserId").alias("Users")).where(size("Users") > 3)

products_for_users_df = productId_for_users_df.withColumn(
    "Products", array("ProductID"))

for i in range(2):
    products_for_users_df = products_for_users_df.withColumnRenamed("Users", "Users1")\
        .withColumnRenamed("Products", "Products1") \
        .crossJoin(products_for_users_df.withColumnRenamed("Users", "Users2")
                   .withColumnRenamed("Products", "Products2")) \
        .where(F.col("Products1") < F.col("Products2"))

    products_for_users_df = products_for_users_df.select(array_union("Products1", "Products2").alias(
        "Products"), array_intersect("Users1", "Users2").alias("Users")).distinct()
    products_for_users_df = products_for_users_df.where(size("Products") >= 2)

# products_for_users_df = products_for_users_df.orderBy(df["Users"][0])

products_for_users_df = products_for_users_df.select(products_for_users_df["Products"].cast(
    "string"), products_for_users_df["Users"].cast("string"))
products_for_users_df.show(10)
products_for_users_df.write.csv(output_filepath, header=True)
