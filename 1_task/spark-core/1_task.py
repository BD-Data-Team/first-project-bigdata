#!/usr/bin/env python3
"""spark application"""

import argparse
import csv
from datetime import datetime
from collections import Counter

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


def get_year_and_product(line):
    line = dict(zip(cols, [a.strip() for a in next(csv.reader([line]))]))

    try:
        year = str(datetime.fromtimestamp(
            int(line['Time'])).year)  # year of the review
    except:
        return None

    return (year, line["ProductId"])


def get_product_and_text(line):
    line = dict(zip(cols, [a.strip() for a in next(csv.reader([line]))]))

    return (line["ProductId"], line["Text"])


year_product_RDD = input_RDD.map(get_year_and_product)
product_text_RDD = input_RDD.map(get_product_and_text)

year_product_RDD = year_product_RDD.filter(
    lambda line: line != None)

# output of this transformation: (year, [(product_id, text), (product_id, text), ...]) for each year
most_reviewed_RDD = year_product_RDD.groupByKey()

most_reviewed_RDD = most_reviewed_RDD.map(lambda line: (
    line[0], [productId for productId, _ in Counter(line[1]).most_common(10)]))

most_reviewed_RDD = most_reviewed_RDD.flatMap(
    lambda line: [(productId, line[0]) for productId in line[1]])

most_reviewed_RDD = most_reviewed_RDD.join(product_text_RDD)

most_reviewed_RDD = most_reviewed_RDD.map(
    lambda line: (line[1][0], (line[0], line[1][1])))

print(most_reviewed_RDD.take(10))

# appreciation_RDD.saveAsTextFile(output_filepath)
