#!/usr/bin/env bash

# get the directory of the current script
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# start the HDFS and YARN daemons if they are not already running
. $SCRIPT_DIR/setup_env.sh


# create the input and output directories
hdfs dfs -mkdir -p $HDFS_INPUT_DIR_PATH
hdfs dfs -mkdir -p $HDFS_OUTPUT_DIR_PATH


# put the dataset into the input directory in HDFS
# for file in /media/francesco/A404F3C504F3990E/Users/stefa/Documenti/datasets_progetto_big_data/datasets/*
for file in $FS_DATASETS_DIR/*
do
    echo $file

    # get the file name
    filename=$(basename -- "$file")
    # get the extension of the file
    extension="${filename##*.}"

    # check if the file is a csv file and if it is a dataset file
    if [[ $extension != "csv" || $filename != $DATASET_PREFIX* ]]
    then
        continue
    fi

    # put the dataset into the input directory in HDFS
    hdfs dfs -put $file $HDFS_INPUT_DIR_PATH/$filename
done

