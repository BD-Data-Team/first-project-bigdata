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

# output task suffix
export TASK_SUFFIX=_task

export HDFS_DATASET_PATH=$HDFS_INPUT_DIR_PATH/dataset.csv
export FS_DATASET_PATH=$MAIN_DIR/datasets/Reviews_cleaned.csv

## start the HDFS and YARN daemons if they are not already running ##
if  [[ $(jps | grep "NameNode" | wc -l) -lt 1 ]] ; then
    echo "Starting DFS and YARN daemons"
    $HADOOP_HOME/sbin/start-dfs.sh && \
    $HADOOP_HOME/sbin/start-yarn.sh
fi

export HADOOP_STREAMING_PATH=$HADOOP_HOME/streaming
export HADOOP_STREAMING_JAR_PATH="$HADOOP_STREAMING_PATH/$( ls $HADOOP_STREAMING_PATH | grep .jar | head -n 1 )"
