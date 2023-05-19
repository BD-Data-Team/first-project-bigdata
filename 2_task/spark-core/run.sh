#!/usr/bin/env bash

spark-submit --master yarn \
        $TASK_DIR/2_task.py \
        --input_path $HDFS_DATASET_PATH \
        --output_path $HDFS_FULL_TASK_OUTPUT_DIR_PATH
