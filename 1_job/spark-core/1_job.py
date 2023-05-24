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
    .appName("Fist task") \
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
        year = int(line['Year'])  # year of the review
    except:
        return None

    return (year, line["ProductId"])


def get_product_and_text(line):
    line = dict(zip(cols, [a.strip() for a in next(csv.reader([line]))]))

    try:
        year = int(line['Year'])  # year of the review
    except:
        return None

    return ((year, line["ProductId"]), line["Text"])


year_product_RDD = input_RDD.map(get_year_and_product)
product_text_RDD = input_RDD.map(get_product_and_text)

year_product_RDD = year_product_RDD.filter(
    lambda line: line != None)

# output of this transformation: (year, [product_id, productmap_id, ...]) for each year
most_reviewed_RDD = year_product_RDD.groupByKey()


# output of this transformation: ((year, productId),None), ((year, productId),None)) ...
top_10_product_for_year_RDD = most_reviewed_RDD.map(lambda line: (
    line[0], [productId for productId, _ in Counter(line[1]).most_common(10)]))\
    .flatMap(lambda line: [((line[0], productId), None) for productId in line[1]])

# output of this transformation: ((year, productId),(None, [text, text, text])
top_10_for_year_with_reviews_RDD = top_10_product_for_year_RDD.join(
    product_text_RDD)

# get away the None values
top_10_for_year_with_reviews_RDD = top_10_for_year_with_reviews_RDD.map(
    lambda line: (line[0], line[1][1]))

top_10_for_year_with_reviews_RDD = top_10_for_year_with_reviews_RDD.groupByKey()

# output of this transformation (year, productId, [(word, count), (word, count), ...]) list of 5 most common words for each product


def get_most_common_words(line):
    words = []
    for text in line[1]:
        words.extend(text.split(" "))
    words = [word for word in words if len(word) >= 4]
    return (line[0], Counter(words).most_common(5))


output_RDD = top_10_for_year_with_reviews_RDD.map(get_most_common_words)
output_RDD = output_RDD.sortByKey()

output_RDD = output_RDD.flatMap(
    lambda line: [f"{line[0][0]}\t{line[0][1]}\t{word}\t{count}"for word, count in line[1]])

output_RDD.saveAsTextFile(output_filepath)
