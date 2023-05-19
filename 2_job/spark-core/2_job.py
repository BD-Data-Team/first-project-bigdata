#!/usr/bin/env python3
"""spark application"""

import argparse
import csv

# create parser and set its arguments
from pyspark.sql import SparkSession


cols = ['Id', 'ProductId', 'UserId', 'ProfileName', 'HelpfulnessNumerator',
        'HelpfulnessDenominator', 'Score', 'Time', 'Summary', 'Text']

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
    .appName("Second task") \
    .getOrCreate()

# spark.sparkContext.textFile(filepath) returns an RDD
# with a record for each line in the input file
input_RDD = spark.sparkContext.textFile(input_filepath).cache()

# skip the first line of the CSV
header = input_RDD.first()
input_RDD = input_RDD.filter(lambda line: line != header)


def transform_data(line):
    line = dict(zip(cols, [a.strip() for a in next(csv.reader([line]))]))

    try:
        line["Helpfulness"] = int(
            line["HelpfulnessNumerator"]) / int(line["HelpfulnessDenominator"])
    except:
        line["Helpfulness"] = None
        return line

    return line


extracted_RDD = input_RDD.map(transform_data)
extracted_RDD = extracted_RDD.filter(
    lambda line: line["Helpfulness"] != None and line["Helpfulness"] <= 1.0)

helpfulness_RDD = extracted_RDD.map(
    lambda line: (line["UserId"], (line["Helpfulness"], 1)))


appreciation_RDD = helpfulness_RDD.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1]))
appreciation_RDD = appreciation_RDD.map(
    lambda line: (line[0], line[1][0] / line[1][1]))

appreciation_RDD = appreciation_RDD.sortBy(
    lambda line: line[1], ascending=False)

appreciation_RDD = appreciation_RDD.map(lambda line:
                                        f"{line[0]}\t{line[1]}")

appreciation_RDD.saveAsTextFile(output_filepath)
