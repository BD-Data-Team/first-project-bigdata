#!/usr/bin/env bash

# get the directory of the current script
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# start the HDFS and YARN daemons if they are not already running
. $SCRIPT_DIR/start_env.sh

# create the input and output directories
hdfs dfs -mkdir -p $HDFS_INPUT_DIR_PATH
hdfs dfs -mkdir -p $HDFS_OUTPUT_DIR_PATH

# put the dataset into the input directory in HDFS
hdfs dfs -put $FS_DATASET_PATH $HDFS_DATASET_PATH
