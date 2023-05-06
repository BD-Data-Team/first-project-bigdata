#!/usr/bin/env bash

hadoop jar $HADOOP_STREAMING_JAR_PATH \
            -mapper $TASK_DIR/mapper.py -reducer $TASK_DIR/reducer.py \
            -input $HDFS_DATASET_PATH -output $HDFS_FULL_TASK_OUTPUT_DIR_PATH