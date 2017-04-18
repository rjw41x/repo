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

# set host to old system
export PGHOST=$OLD_MASTER

# get list of schemas
SCHEMAS=$(psql -t -c "select schema_name from information_schema.schemata where schema_name not in ( 'gp_toolkit', 'pg_toast', 'pg_bitmapindex', 'pg_aoseg', 'pg_catalog', 'information_schema', 'ext');" )

# create schema ext ignore error if it exists
psql -d $DB -c "create schema ext;" > /dev/null 2>&1

# MAIN
for schema in $SCHEMAS
do
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
            location ('gpfdist://${NEW_SEGS}:${WRITE_PORT1}/${table}.psv','gpfdist://${NEW_SEGS}:${WRITE_PORT2}/${table}.psv' )
            format 'text' ( delimiter '|' null ''  );" > /tmp/cr_ext.sql
            # RJW - need to account for multiple ext table hosts based on NEW_SEGMENTs
            # location ('gpfdist://v3_sdw1:${WRITE_PORT1}/${table}.psv','gpfdist://v3_sdw1:${WRITE_PORT2}/${table}.psv',
            # 'gpfdist://v3_sdw2:${WRITE_PORT1}/${table}.psv','gpfdist://v3_sdw2:${WRITE_PORT2}/${table}.psv',
            # 'gpfdist://v3_sdw3:${WRITE_PORT1}/${table}.psv','gpfdist://v3_sdw3:${WRITE_PORT2}/${table}.psv',
            # 'gpfdist://v3_sdw4:${WRITE_PORT1}/${table}.psv','gpfdist://v3_sdw4:${WRITE_PORT2}/${table}.psv')
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
        rm /tmp/cr_ext.sql /tmp/cr_ext.out

    # end if SKIP_EXT
    fi

    # UNLOAD TO EXT TABLES
    for table in $TABLES
    do
        echo "insert into ext.$table select * from ${schema}.${table};" > /tmp/unload.sql
        psql -d $DB -f /tmp/unload.sql > /tmp/unload.out 2>&1
        sql_error /tmp/unload.out /tmp/unload.sql 
        if [[ $RET_VAL == 0 ]]; then
            log "SUCCESS: table $table unload"
        else
            message "FAIL: table $table unload"
            continue
        fi
        # build func to set remote semaphore
        # for now set local semaphore
        touch /tmp/${schema}.${table}.rdy
        # exit 1
    done

    # clean up
    rm /tmp/unload.sql /tmp/unload.out

    # clear variable
    TABLES=""
done

