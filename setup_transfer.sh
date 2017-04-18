#!/bin/bash
# set -x

# VARIABLES
REDO_LOG=~/logs/setup_redo
LOGFILE=~/logs/setup_transfer
source master_conf.sh
# FUNCTIONS
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
# make sure we can log the output
if [[ ! -d $ARTIFACT_DIR ]]; then
    mkdir $ARTIFACT_DIR 
    if [[ $? != 0 ]]; then
        message "Cannot create artifact directory $ARTIFACT_DIR"
        exit 1
    fi
fi
if [[ ! -d ~/logs ]]; then
    mkdir ~/logs
    if [[ $? != 0 ]]; then
        message "Cannot create logs directory ~/logs"
        exit 1
    fi
fi
# Access the old system first
export PGHOST=$OLD_MASTER
export PGPORT=$OLD_PORT

# validate connectivity from all hosts to all hosts

# get the instance globals
message "dumping globals pg_dumpall -l $DB -g $ARTIFACT_DIR/${DB}.globals.sql"
pg_dumpall -l $DB -g > $ARTIFACT_DIR/${DB}.globals.sql
if [[ $? != 0 ]]; then
    message "Error dumping globals using pg_dumpall, aborting"
    exit 1
fi

# get schemas, tables - including ownership - and all other non-globabl db objects - by schema
# iterate over schemas
SCHEMAS=$(psql -A -t -c "select schema_name from information_schema.schemata where schema_name not in ( 'gp_toolkit', 'pg_toast', 'pg_bitmapindex', 'pg_aoseg', 'pg_catalog', 'information_schema', 'ext' );" ) # skip system schemas
for schema in $SCHEMAS
do
    message "dumping schema for $DB: pg_dump -s -n $schema $DB  $ARTIFACT_DIR/${DB}.${schema}.ddl"
    pg_dump -s -n $schema $DB > $ARTIFACT_DIR/${DB}.${schema}.ddl
    if [[ $? != 0 ]]; then
        message "failed to dump $schema for $db, aborting"
        exit 1
    fi
done
############################ CHANGING GEARS from unloading to loading ###################
# Now we can reload the information on the new host
export PGHOST=$NEW_MASTER
export PGPORT=$NEW_PORT
export PGDATABASE=postgres
message "DB Settings -- Host $PGHOST, PORT $PGPORT, DB $PGDATABASE"
# DEBUG
# env | grep PG
# exit 1

# create the database
message "creating remote db: create database $DB;" /tmp/db_cr.out
psql -c "create database $DB;" > /tmp/db_cr.out 2>&1
sql_error /tmp/db_cr.out "create db $DB" ABORT

export PGDATABASE=$DB
# restore globals
message "Restoring globals on new host $PGHOST: $ARTIFACT_DIR/${DB}.globals.sql" /tmp/globals_restore.out
psql -f $ARTIFACT_DIR/${DB}.globals.sql > /tmp/globals_restore.out 2>&1
SQL_OUT=$?
# sql_error /tmp/globals_restore.out $ARTIFACT_DIR/${DB}.globals.sql ABORT
grep 'already exists' /tmp/globals_restore.out > /dev/null 2>&1
if [[ $? == 0 ]]; then
    message "some roles already existed, continuing"
else
    if [[ $SQL_OUT == 1 ]]; then
        message "an error occurred restoring globals, check /tmp/globals_restore.out"
        exit 1
    fi
fi

# restore ddl by schema
# errors in sql here are recoverable, so continue even if we error
for schema in $SCHEMAS
do
    message "Creating schema/tables on remote $ARTIFACT_DIR/${DB}.${schema}.ddl" /tmp/schema_${schema}_restore.out
    psql -f $ARTIFACT_DIR/${DB}.${schema}.ddl > /tmp/schema_${schema}_restore.out 2>&1 
    sql_error /tmp/schema_${schema}_restore.out $ARTIFACT_DIR/${DB}.${schema}.ddl 
    # capture the failed db/schema and file in the redo_log
    if [[ $RET_VAL == 1 ]]; then
        redo_log "$DB $schema restore of $ARTIFACT_DIR/${DB}.${schema}.ddl"
    fi
done

message "$(date) $0 completed successfully"
exit 0
