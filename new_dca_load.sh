#!/bin/bash

# master variables
source master_conf.sh

# GLOBALS
LOGFILE=~/logs/load.out
RESULTS=~/load/results.out
REDO_LOG=~/logs/load_redo
mv $REDO_LOG ~/logs/old_load_redo # save the old one in case we weren't done with it.
> $REDO_LOG # initialize it

# FUNCTIONS
file_check() {
    ls /tmp/*.rdy > /dev/null 2>&1
    if [[ $? == 0 ]]; then
        return 1
    else
        return 0
    fi
}
clean_load_files() {
    for seg in $SEGMENTS
    do
        ssh $seg rm -f ${DATA_DIR1}/$1.$2.psv ${DATA_DIR2}/$1.$2.psv
    done
}

# MAIN LOOP - wait for file semaphores to appear
while [[ 1 ]]
do
    ls /tmp/*.rdy > /dev/null 2>&1
    if [[ $? != 0 ]]; then
        # wait for files
        sleep 5
        # continue with next iteration of loop
        continue
    fi
    # else we have a file or more so process them
    for fil in /tmp/*.rdy
    do
        # CREATE EXT TABLES
        schema=$(echo $fil | awk -F"." '{ print $1 }')
        table=$(echo $fil | awk -F"." '{ print $2 }')
        psql -c "drop external table if exists ext.${table};" > /dev/null 2>&1
        echo "create readable external table ext.$table ( like ${schema}.${table} )
        location ('gpfdist://rjw1:${READ_PORT1}/${table}.psv','gpfdist://rjw1:${READ_PORT2}/${table}.psv' )
        format 'text' ( delimiter '|' null ''  );" > /tmp/cr_ext.sql
        # location ('gpfdist://v3_sdw1:8081/${table}.psv','gpfdist://v3_sdw1:8082/${table}.psv',
        # 'gpfdist://v3_sdw2:8081/${table}.psv','gpfdist://v3_sdw2:8082/${table}.psv',
        # 'gpfdist://v3_sdw3:8081/${table}.psv','gpfdist://v3_sdw3:8082/${table}.psv',
        # 'gpfdist://v3_sdw4:8081/${table}.psv','gpfdist://v3_sdw4:8082/${table}.psv')
        psql -f /tmp/cr_ext.sql > /tmp/cr_ext.out 2>&1 
        sql_error /tmp/cr_ext.out /tmp/cr_ext.sql 
        if [[ $RET_VAL != 0 ]]; then
            message "FAIL: create external readable table ext.$table"
            # add the table to the redo log on failure
            redo_log $table 
            continue # goto next table
        else
            log "SUCCESS: external readable table ext.$table"
        fi

        # LOAD FROM EXT TABLES
        echo "insert into ${schema}.${table} select * from ext.${table};" > /tmp/load_from_ext.sql
        psql -f /tmp/load_from_ext.sql > /tmp/load_from_ext.out 2>&1
        sql_error /tmp/load_from_ext.out /tmp/load_from_ext.sql
        if [[ $RET_VAL == 0 ]]; then
            echo ${schema}.${table} | paste - /tmp/load_from_ext.out >> $RESULTS
            clean_load_files ${schema}.${table}
            # remove the semaphore so we don't load it again
            rm $fil
        else
            # capture the schema and table in the redo log
            redo_log ${schema}.${table}
            message "FAIL: load of ${schema}.${table}"
        fi
    done
done
