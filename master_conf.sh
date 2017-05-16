#!/bin/bash

# directory to store artifacts from the transfer process in
export ARTIFACT_DIR=/home/gpadmin/transfer

export DB_GLOBALS_FILE=${ARTIFACT_DIR}/db_globals.sql
export DB_SCHEMAS_FILE=${ARTIFACT_DIR}/db_schemas.sql

# where data will live on the NEW dca segment servers
# data locations:
# /data1 /data2 on each segment server on the NEW V3 hardware
export DATA_DIR1=/data1/trans1  # /data1/transfer
export DATA_DIR2=/data2/trans2  # /data2/transfer
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
export OLD_MASTER=30.135.107.124  # 192.168.177.139
export NEW_MASTER=30.135.107.190  # 192.168.177.138
OLD_PORT=5432
NEW_PORT=5432

# segment names (probably need IP's to make it work w/out DNS)
# export NEW_SEGS="rjw1"
# export OLD_SEGS="rjw1"
# DEV SETUP
export NEW_SEGS="30.135.107.193 30.135.107.194 30.135.107.195 30.135.107.196" 
export OLD_SEGS="30.135.107.125 30.135.107.126 30.135.107.127 30.135.107.128 30.135.107.129 30.135.107.119 30.135.107.121 30.135.107.122"
EXT_SEGS=32

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
# $1 = $? $2 == output file, $3 == sql file name, $4 (opt) ABORT - force exit for sql errors
sql_ext() {
    grep $2 $1 > /dev/null 2>&1
    # 0 = success so we found an external table
    if [[ $? == 0 ]]; then
        EMPTY='NO'
    else
        EMPTY='YES'
    fi
}

ld_error() {
    grep 'ERROR' $2 > /dev/null 2>&1
    if [[ $? == 1 ]]; then
        RET_VAL=0
        return
    fi
    grep -i http $2 > /dev/null 2>&1
    if [[ $? == 1 ]]; then
        message "HTTP Error"
        RET_VAL=2
    else
        message "unknown error: $(cat $2 $3)"
        RET_VAL=1
    fi
    return
# psql:/tmp/load_from_ext.sql:1: ERROR:  http response code 404 from gpfdist (gpfdist://30.135.107.194:8088/clm_rcvy_lz.cor_adj_status_cd_lz.psv): HTTP/1.0 404 file not found (url.c:354)  (seg26 slice1 sdw4.gphd.local:1027 pid=467394) (cdbdisp.c:1322)
}
sql_error() {
    SQL_ERR=$1
    egrep -i 'ERROR|FATAL' $2 > /dev/null 2>&1
    if [[ $? != 1 ]]; then
        RET_VAL=1
        message "error running sql $3"
        if [[ ! -z "$4" ]]; then
            exit $RET_VAL
        fi
    else
        RET_VAL=0
    fi
}
