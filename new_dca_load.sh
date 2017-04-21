#!/bin/bash

usage () {
    cat << EOF
    usage:  $0 database_name
EOF
exit 1
}
# check args
if [[ $# != 1 ]]; then
    usage
else
    DB=$1
fi

# GLOBALS
LOGFILE=~/logs/load.out
RESULTS=~/logs/results.out
REDO_LOG=~/logs/load_redo
mv $REDO_LOG ~/logs/old_load_redo > /dev/null 2>&1 # save the old one in case we weren't done with it.
> $REDO_LOG # initialize it

# master variables
source master_conf.sh

# FUNCTIONS
file_check() {
    ls /tmp/*.rdy > /dev/null 2>&1
    if [[ $? != 0 ]]; then
        return 1
    else
        return 0
    fi
}
clean_load_files() {
    for seg in $NEW_SEGS
    do
        # message "cleaning load files for $1 $2"
        message "ssh $seg \"rm -f ${DATA_DIR1}/$1.$2.psv ${DATA_DIR2}/$1.$2.psv\""
        ssh $seg "rm -f ${DATA_DIR1}/$1.$2.psv ${DATA_DIR2}/$1.$2.psv"
    done
}

message $(date) $0 start
# check to see that gpfdist procs are running
./manage_gpfdist.sh check > /dev/null 2>&1
if [[ $? != 0 ]]; then
    # try to start them then check again
    ./manage_gpfdist.sh start > /dev/null 2>&1
    ./manage_gpfdist.sh check > /dev/null 2>&1
    if [[ $? != 0 ]]; then
        message "gpfdist processes are not running properly, restart failed, exiting"
        exit 1
    fi
fi
 
export PGHOST=$NEW_MASTER
export PGPORT=$NEW_PORT
# psql -c "select count(*) from bball.appearances;" # debug to insure we are connecting to the right system ;-)
# create schema ext ignore error if it exists
psql -d $DB -c "create schema ext;" > /dev/null 2>&1

# MAIN LOOP - wait for file semaphores to appear
while [[ 1 ]]
do
    if [[ -e /tmp/complete ]]; then
        message "complete file found, exiting"
        break
    fi
    ls /tmp/*.rdy > /dev/null 2>&1
    if [[ $? != 0 ]]; then
	echo waiting for files
        # wait for files
        sleep 5
        # continue with next iteration of loop
        continue
    fi
    # else we have a file or more so process them in the order they arrive
    for fil in $(ls -1ct /tmp/*.rdy)
    do
        # CREATE EXT TABLES
        schema=$(echo $fil | awk -F"/" '{ x=split($3,sch,"."); printf("%s", sch[1] ); }')
        table=$(echo $fil | awk -F"/" '{ x=split($3,tbl,"."); printf("%s", tbl[2] ); }')
        message "processing schema -${schema}- table -${table}-"

        psql -d $DB -c "drop external table if exists ext.${table};" > /dev/null 2>&1
        echo "create readable external table ext.$table ( like ${schema}.${table} ) location (" > /tmp/cr_ext.sql
        cnt=0
        num_segs=$(echo $NEW_SEGS | wc -w)
        for seg in $NEW_SEGS
        do
            cnt=$((cnt+1))
            if [[ $num_segs == $cnt ]]; then
                echo "'gpfdist://${seg}:${READ_PORT1}/${schema}.${table}.psv','gpfdist://${seg}:${READ_PORT2}/${schema}.${table}.psv'"  >> /tmp/cr_ext.sql
            else
                echo "'gpfdist://${seg}:${READ_PORT1}/${schema}.${table}.psv','gpfdist://${seg}:${READ_PORT2}/${schema}.${table}.psv',"  >> /tmp/cr_ext.sql
            fi
        done
        echo ") format 'text' ( delimiter '|' null ''  );" >> /tmp/cr_ext.sql
        psql -d $DB -f /tmp/cr_ext.sql > /tmp/cr_ext.out 2>&1 
        sql_error /tmp/cr_ext.out /tmp/cr_ext.sql 
        if [[ $RET_VAL != 0 ]]; then
            message "FAIL: create external readable table ext.$table"
            # add the table to the redo log on failure
            redo_log ${schema}.${table} ext table failed 
            continue # goto next table
        else
            log "SUCCESS: external readable table ext.$table"
        fi

        # LOAD FROM EXT TABLES
        echo "insert into ${schema}.${table} select * from ext.${table};" > /tmp/load_from_ext.sql
        psql -d $DB -f /tmp/load_from_ext.sql > /tmp/load_from_ext.out 2>&1
        sql_error /tmp/load_from_ext.out /tmp/load_from_ext.sql
        if [[ $RET_VAL == 0 ]]; then
            echo ${schema}.${table} $(egrep 'INSERT|COPY' /tmp/load_from_ext.out) >> $RESULTS
            clean_load_files ${schema} ${table}
            message "SUCCESS: ${schema}.${table} loaded"
            # remove the semaphore so we don't load it again
            rm $fil
        else
            # capture the schema and table in the redo log
            redo_log ${schema}.${table} load failed
            message "FAIL: ${schema}.${table} on load "
            rm $fil
        fi
    done
done

message $(date) $0 complete
