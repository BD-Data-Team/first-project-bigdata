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

header = input_RDD.first()
input_RDD = input_RDD.filter(lambda line: line != header)

def all_in_one(line):
    line = dict(zip(cols, [a.strip() for a in next(csv.reader([line]))]))
    line["Helpfulness"] = -1

    if (float(line['HelpfulnessDenominator']) <= 0 ):
        return line
    
    try:
        line["Helpfulness"] = float(line["HelpfulnessNumerator"]) / float(line["HelpfulnessDenominator"])
    except:
        pass

    if line["Helpfulness"] > 1:
        line["Helpfulness"] = -1

    return line


extracted_RDD = input_RDD.map(all_in_one)
extracted_RDD = extracted_RDD.filter(lambda line: line["Helpfulness"] >= 0)

helpfulness_RDD = extracted_RDD.map(lambda line: (line["UserId"], (line["Helpfulness"], 1)))


appreciation_RDD = helpfulness_RDD.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + a[1]))
appreciation_RDD = appreciation_RDD.map(lambda line: f"{line[0]}  {line[1][0] / line[1][1]}")

appreciation_RDD = appreciation_RDD.sortBy(lambda line: line[1], ascending=False)

appreciation_RDD.saveAsTextFile(output_filepath)