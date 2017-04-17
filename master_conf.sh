#!/bin/bash

# directory to store artifacts from the transfer process in
export ARTIFACT_DIR=/home/gpadmin/transfer

export DB_GLOBALS_FILE=${ARTIFACT_DIR}/db_globals.sql
export DB_SCHEMAS_FILE=${ARTIFACT_DIR}/db_schemas.sql

# where data will live on the NEW dca segment servers
# data locations:
# /data1 /data2 on each segment server on the NEW V3 hardware
export DATA_DIR1=/data/gpdata1/trans1  # /data1/transfer
export DATA_DIR2=/data/gpdata2/trans2  # /data2/transfer
# requires starting 2 gpfdist for writes on each V3 Segment server - 8081 (/data1), 8082 (/data2)
# requires starting 2 gpfdist for reads on each V3 Segment server - 8088 (/data1), 8089 (/data2)
# Ports that writable ext tables will use
export WRITE_PORT1=8081
export WRITE_PORT2=8082
# Ports that readable ext tables will use
export READ_PORT1=8088
export READ_PORT2=8089
# default location of gpfdist logs
export GPFDLOGFILE=~/logs/gpfd.log

# GPDB Connection Info
export OLD_MASTER=192.168.177.131
export NEW_MASTER=192.168.177.133
OLD_PORT=5432
NEW_PORT=5432

# segment names (probably need IP's to make it work w/out DNS)
# export NEW_SEGS="sdw1 sdw2 sdw3 sdw4"
export NEW_SEGS="rjw2"
# export OLD_SEGS="sdw1 sdw2 sdw3 sdw4 sdw5 sdw6 sdw7 sdw8 sdw9 sdw10 sdw11 sdw12 sdw13 sdw14 sdw15 sdw16"
export OLD_SEGS="rjw1"

# return value for functions - clear it when sourced
RET_VAL=""
# FUNCTIONS
log () {
    echo "$*" >> "$LOGFILE"
}
message() {
    log $*
    echo $*
}
redo_log() {
    echo $* >> $REDO_LOG
}
# $1 == output file, $2 == sql file name, $3 (opt) ABORT - force exit for sql errors
sql_error() {
    SQL_ERR=$?
    egrep 'ERROR|FATAL' $1 > /dev/null 2>&1
    if [[ $? != 1 ]]; then
        RET_VAL=1
        message "error running sql $2"
        if [[ ! -z "$3" ]]; then
            exit $RET_VAL
        fi
    else
        RET_VAL=0
    fi
}
