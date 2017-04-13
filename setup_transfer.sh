#!/bin/bash

source master_conf.sh

# FUNCTIONS

if [[ ! -d $ARTIFACT_DIR ]]; then
    mkdir $ARTIFACT_DIR 
    if [[ $? != 0 ]]; then
        message "Cannot create artifact directory $ARTIFACT_DIR"
        exit 1
    fi
fi

# get the instance globals
pg_dumpall -g > 
