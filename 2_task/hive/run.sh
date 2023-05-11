#!/usr/bin/env bash

TASK_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# cd ~/Dev/hive/ # questo solo per me (Davide)
cd
hive -f $TASK_DIR/$TASK_DIR_NAME.hql
cd $MAIN_DIR