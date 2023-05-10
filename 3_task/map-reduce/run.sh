#!/usr/bin/env bash

TASK_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

hadoop jar $HADOOP_STREAMING_JAR_PATH \
            -mapper $TASK_DIR/mapper.py -reducer $TASK_DIR/reducer.py \
            -input $HDFS_DATASET_PATH -output $HDFS_FULL_TASK_OUTPUT_DIR_PATH