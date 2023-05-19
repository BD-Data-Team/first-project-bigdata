#!/usr/bin/env bash

## initializing the environment variables ##

# get the directory of the current script
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

export MAIN_DIR=$SCRIPT_DIR/..
# data-team home directory
export DATA_TEAM_HOME=hdfs:///user/data-team

# input and output directories
export HDFS_INPUT_DIR_PATH=$DATA_TEAM_HOME/input
export HDFS_OUTPUT_DIR_PATH=$DATA_TEAM_HOME/output

# output job suffix
export JOB_SUFFIX=_job

export FS_DATASETS_DIR=$MAIN_DIR/datasets

export DATASET_PREFIX=dataset_

export HADOOP_STREAMING_PATH=$HADOOP_HOME/streaming
export HADOOP_STREAMING_JAR_PATH="$HADOOP_STREAMING_PATH/$( ls $HADOOP_STREAMING_PATH | grep .jar | head -n 1 )"