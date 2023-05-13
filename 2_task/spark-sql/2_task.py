#!/usr/bin/env python3
"""spark application"""

import argparse

# create parser and set its arguments
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# initialize SparkSession
# with the proper configuration
spark = SparkSession \
    .builder \
    .config("spark.driver.host", "localhost") \
    .appName("Second task") \
    .getOrCreate()

# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

df = spark.read.csv(input_filepath, header=True, inferSchema=True)

df = df.withColumn("Helpfulness", df["HelpfulnessNumerator"] / df["HelpfulnessDenominator"]) \
    .groupBy("UserId").agg(F.avg("Helpfulness").alias("Appreciation"))

df = df.filter(df["Appreciation"] <= 1.0) \
    .sort("Appreciation", ascending=False)

df.write.csv(output_filepath, header=True)

