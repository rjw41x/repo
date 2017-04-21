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
# RJW - may be a hardcoded list in production
LOGFILE=~/logs/unload.out
REDO_LOG=~/logs/unload_redo
mv $REDO_LOG ~/logs/old_redo # save the old one in case we weren't done with it.
> $REDO_LOG # initialize it

# get project control information - have to source after setting local variables ;-)
source master_conf.sh

message $(date) $0 start
# set host to old system
export PGHOST=$OLD_MASTER

# get list of schemas
SCHEMAS=$(psql -t -c "select schema_name from information_schema.schemata where schema_name not in ( 'gp_toolkit', 'pg_toast', 'pg_bitmapindex', 'pg_aoseg', 'pg_catalog', 'information_schema', 'ext');" )

# create schema ext ignore error if it exists
psql -d $DB -c "create schema ext;" > /dev/null 2>&1

# MAIN
for schema in $SCHEMAS
do
    # check to see that gpfdist procs are running
    ./manage_gpfdist.sh restart #  restart gpfdists between schemas
    ./manage_gpfdist.sh check > /dev/null 2>&1
    if [[ $? != 0 ]]; then
        message "gpfdist processes are not running properly, restart failed, exiting"
        exit 1
    fi
    message "processing schema $schema"
    TABLES=$(psql -d $DB -t -c "select table_name from information_schema.tables where table_schema = '$schema';")
    if [[ -z "$SKIP_EXT" ]]; then
        # CREATE EXT TABLES
        for table in $TABLES
        do
            # debug
            # echo "  $table"
            psql -d $DB -c "drop external table if exists ext.${table};" > /dev/null 2>&1
            echo "create writable external table ext.$table ( like ${schema}.${table} )
            location (" > /tmp/cr_ext.sql
            cnt=0
            num_segs=$(echo $NEW_SEGS | wc -w)
            for seg in $NEW_SEGS
            do
                cnt=$((cnt+1))
                if [[ $num_segs == $cnt ]]; then
                    echo "'gpfdist://${seg}:${WRITE_PORT1}/${schema}.${table}.psv','gpfdist://${seg}:${WRITE_PORT2}/${schema}.${table}.psv'" >> /tmp/cr_ext.sql 
                else
                    echo "'gpfdist://${seg}:${WRITE_PORT1}/${schema}.${table}.psv','gpfdist://${seg}:${WRITE_PORT2}/${schema}.${table}.psv'," >> /tmp/cr_ext.sql 
                fi
                    
            done
            echo ") format 'text' ( delimiter '|' null ''  );" >> /tmp/cr_ext.sql
            # cat /tmp/cr_ext.sql # DEBUG
            # exit 1
            # create the table, trapping any errors
            psql -d $DB -f /tmp/cr_ext.sql > /tmp/cr_ext.out 2>&1 
            sql_error /tmp/cr_ext.out /tmp/cr_ext.sql 
            if [[ $RET_VAL != 0 ]]; then
                message "FAIL: external table ext.$table"
                # add the table to the redo log on failure
                redo_log $table 
                continue # goto next table
            else
                log "SUCCESS: external table ext.$table"
            fi

            # do we want to do anything else in this script?
        done
        # clean up between schemas
        rm /tmp/cr_ext.sql /tmp/cr_ext.out > /dev/null 2>&1

    # end if SKIP_EXT
    fi

    # UNLOAD TO EXT TABLES
    for table in $TABLES
    do
        rows=$(psql -t -c "SELECT reltuples::BIGINT AS estimate FROM pg_class WHERE oid='${schema}.${table}'::regclass;")
        if [[ $rows == 0 ]]; then
            message "Table ${schema}.${table} appears to be empty, skipping.  Validate"
            redo_log ${schema}.${table} Table shows as empty
            continue
        fi
        echo "insert into ext.$table select * from ${schema}.${table};" > /tmp/unload.sql
        psql -d $DB -f /tmp/unload.sql > /tmp/unload.out 2>&1
        sql_error /tmp/unload.out /tmp/unload.sql 
        if [[ $RET_VAL == 0 ]]; then
            log "SUCCESS: table ${schema}.${table} unload"
        else
            message "FAIL: table ${schema}.${table} unload"
            redo_log ${schema}.${table} unload failed
            continue
        fi
        # build func to set remote semaphore
        # for now set local semaphore
        echo $rows > /tmp/${schema}.${table}.rdy
        # exit 1
    done

    # clean up
    rm /tmp/unload.sql /tmp/unload.out > /dev/null 2>&1

    # clear variable for next iteration
    TABLES=""
done
message $(date) $0 complete
