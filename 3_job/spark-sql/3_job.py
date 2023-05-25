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

input_DF = spark.read.csv(input_filepath, header=True,
                          inferSchema=True).cache()

productId_for_users_DF = input_DF.select("UserId", "ProductId").where(input_DF["Score"] >= 4) \
    .groupBy("ProductId").agg(collect_set("UserId").alias("Users")).where(size("Users") > 3)

products_for_users_DF = productId_for_users_DF.withColumn(
    "Products", array("ProductID"))

for i in range(2):
    products_for_users_DF = products_for_users_DF.withColumnRenamed("Users", "Users1")\
        .withColumnRenamed("Products", "Products1") \
        .crossJoin(products_for_users_DF.withColumnRenamed("Users", "Users2")
                   .withColumnRenamed("Products", "Products2")) \
        .where(F.col("Products1") < F.col("Products2"))

    products_for_users_DF = products_for_users_DF.select(array_union("Products1", "Products2").alias(
        "Products"), array_intersect("Users1", "Users2").alias("Users")).distinct()
    products_for_users_DF = products_for_users_DF.where(size("Products") >= 2)

output_DF = products_for_users_DF.groupBy("Products").orderBy(
    products_for_users_DF["Users"][0])

output_DF = output_DF.select(output_DF["Products"].cast(
    "string"), output_DF["Users"].cast("string"))
# products_for_users_DF.show(10)
output_DF.write.csv(output_filepath, header=True)
