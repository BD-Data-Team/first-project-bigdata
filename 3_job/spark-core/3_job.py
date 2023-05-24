#!/usr/bin/env python3
"""spark application"""

import argparse
import csv
from collections import Counter

# create parser and set its arguments
from pyspark.sql import SparkSession


cols = ['Id', 'ProductId', 'UserId', 'ProfileName', 'HelpfulnessNumerator',
        'HelpfulnessDenominator', 'Score', 'Year', 'Summary', 'Text']

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
    .appName("Third task") \
    .getOrCreate()

# spark.sparkContext.textFile(filepath) returns an RDD
# with a record for each line in the input file
input_RDD = spark.sparkContext.textFile(input_filepath).cache()

# skip the first line of the CSV
header = input_RDD.first()
input_RDD = input_RDD.filter(lambda line: line != header)


def get_users_for_productId(line):
    line = dict(zip(cols, [a.strip() for a in next(csv.reader([line]))]))

    try:
        line["Score"] = int(line["Score"])  # year of the review
    except:
        return None

    if line["Score"] < 4:
        return None

    return (line["ProductId"], line["UserId"])


productId_for_users_RDD = input_RDD.map(get_users_for_productId)

productId_for_users_RDD = productId_for_users_RDD.filter(
    lambda line: line != None)

# output of this transformation: (productId, [user_id, user_id, user_id, ...]) list of users that reviewed the product
products_for_users_RDD = productId_for_users_RDD.groupByKey().map(
    lambda line: (set([line[0]]), set(line[1])))

# with the first cartesian product we get all products that have at least 2 users in common
# with the second we get all products that have at least 3 users in common
for i in range(2):
    # output of this transformation: ((productId, [user_id, user_id, user_id, ...]), (productId, [user_id, user_id, user_id, ...]))
    products_for_users_RDD = products_for_users_RDD.cartesian(
        products_for_users_RDD)
    products_for_users_RDD = products_for_users_RDD.filter(
        lambda x: x[0][0] != x[1][0])
    # output of this transformation: ([productId,productId, ...], [user_id, user_id, user_id, ...])
    products_for_users_RDD = products_for_users_RDD.map(lambda x: (x[0][0].union(
        x[1][0]), x[0][1].intersection(set(x[1][1]))))
    products_for_users_RDD = products_for_users_RDD.filter(
        lambda line: len(line[1]) >= 2)

# products_for_users_RDD = products_for_users_RDD.sortBy(
#     lambda line: list(line[1])[0])

# products_for_users_RDD = products_for_users_RDD.groupBy(
#     lambda line: line[0])

print(products_for_users_RDD.take(10))

# TODO:  Il risultato deve essere ordinato in base allo UserId del primo elemento del gruppo e non devono essere presenti duplicati.

# products_for_users_RDD.saveAsTextFile(output_filepath)
