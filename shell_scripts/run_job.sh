#!/usr/bin/env bash

# $1 is the job number
# $2 is the implementation name
# $3 peercentage of the dataset to be used

# check the number of arguments
if [[ $# -ne 3 ]] ; then
    echo "Usage: ./run_job.sh <JOB_number> <implementation_name> <percentage_of_dataset_to_be_used>"
    exit 1
fi

# get the directory of the current script
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# initialize all the environment variables in the current shell
. $SCRIPT_DIR/setup_env.sh 

# get the JOB directory name
JOB_DIR_NAME=$1$JOB_SUFFIX

# get the dataset path
DATASET_NAME=$DATASET_PREFIX$3
HDFS_DATASET_PATH=$HDFS_INPUT_DIR_PATH/$DATASET_NAME.csv

HDFS_JOB_OUTPUT_DIR_PATH=$HDFS_OUTPUT_DIR_PATH/$JOB_DIR_NAME
# get the JOB output directory path
HDFS_FULL_JOB_OUTPUT_DIR_PATH=$HDFS_OUTPUT_DIR_PATH/$JOB_DIR_NAME/$2


# remove the JOB output directory of the current implementation JOB from HDFS
echo "Removing the output directory $HDFS_FULL_JOB_OUTPUT_DIR_PATH from HDFS"
hdfs dfs -rm -r $HDFS_FULL_JOB_OUTPUT_DIR_PATH


JOB_DIR=$MAIN_DIR/$JOB_DIR_NAME/$2
. $JOB_DIR/run.sh