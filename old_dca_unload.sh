#!/bin/bash

# get project control information
source master_conf.sh

# GLOBALS
# RJW - may be a hardcoded list in production
SCHEMAS=$(psql -t -c"select schema_name from information_schema.schemata where schema_name not in ( 'gp_toolkit', 'pg_toast', 'pg_bitmapindex', 'pg_aoseg', 'pg_catalog', 'information_schema', 'ext', 'public');" )
LOGFILE=~/logs/unload.out
REDO_LOG=~/logs/redo
mv $REDO_LOG ~/logs/old_redo # save the old one in case we weren't done with it.
> $REDO_LOG # initialize it

# FUNCTIONS
log () {
    echo $* >> $LOGFILE
}
message() {
    log $*
    echo $*
}
redo_log() {
    echo $* >> $REDO_LOG
}
sql_error() {
    grep ERROR $1 > /dev/null 2>&1
    if [[ $? != 0 ]]; then
        return 0
    else
        return 1
    fi
}

# MAIN
for schema in $SCHEMAS
do
    log "processing schema $schema"
	TABLES=$(psql -t -c "select table_name from information_schema.tables where table_schema = '$schema';")

    if [[ -z "$SKIP_EXT" ]]; then
        # CREATE EXT TABLES
        for table in $TABLES
        do
            # debug
            # echo "  $table"
            psql -c "drop external table if exists ext.${table};" > /dev/null 2>&1
            echo "create writable external table ext.$table ( like ${schema}.${table} )
            location ('gpfdist://rjw1:${WRITE_PORT1}/${table}.psv','gpfdist://rjw1:${WRITE_PORT2}/${table}.psv' )
            format 'text' ( delimiter '|' null ''  );" > /tmp/cr_ext.sql
            # location ('gpfdist://v3_sdw1:${WRITE_PORT1}/${table}.psv','gpfdist://v3_sdw1:${WRITE_PORT2}/${table}.psv',
            # 'gpfdist://v3_sdw2:${WRITE_PORT1}/${table}.psv','gpfdist://v3_sdw2:${WRITE_PORT2}/${table}.psv',
            # 'gpfdist://v3_sdw3:${WRITE_PORT1}/${table}.psv','gpfdist://v3_sdw3:${WRITE_PORT2}/${table}.psv',
            # 'gpfdist://v3_sdw4:${WRITE_PORT1}/${table}.psv','gpfdist://v3_sdw4:${WRITE_PORT2}/${table}.psv')
            # create the table, trapping any errors
            psql -f /tmp/cr_ext.sql > /tmp/cr_ext.out 2>&1 
            sql_error /tmp/cr_ext.out
            if [[ $? != 0 ]]; then
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
        psql -f /tmp/unload.sql > /tmp/unload.out 2>&1
        sql_error /tmp/unload.out
        if [[ $? == 0 ]]; then
            log "SUCCESS: table $table unload"
        else
            message "FAIL: table $table unload"
            continue
        fi
        # build func to set remote semaphore
        # for now set local semaphore
        touch /tmp/${schema}.${table}.rdy
        exit 1
    done

    # clean up
    rm /tmp/unload.sql /tmp/unload.out

    # clear variable
    TABLES=""
done

