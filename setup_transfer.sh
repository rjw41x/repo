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

if [[ -z "$NO_DUMP_ALL" ]]; then
# get the instance globals
    message "dumping globals pg_dumpall -l $DB -g $ARTIFACT_DIR/${DB}.globals.sql"
    pg_dumpall -l $DB -g > $ARTIFACT_DIR/${DB}.globals.sql
    if [[ $? != 0 ]]; then
        message "Error dumping globals using pg_dumpall, aborting"
        exit 1
    fi
fi

# get schemas, tables - including ownership - and all other non-globabl db objects - by schema
# iterate over schemas
SCHEMAS=$(psql -A -t -c "select schema_name from information_schema.schemata where schema_name not in ( 'gp_toolkit', 'pg_toast', 'pg_toast_temp_1', 'pg_bitmapindex', 'pg_aoseg', 'pg_catalog', 'information_schema', 'ext','madlib');" $DB ) # skip system schemas
# SCHEMAS="clm_rcvy_stg_prj clm_rcvy_buss_sit clm_rcvy_lz clm_rcvy_lz_prj clm_rcvy_lz_sit clm_rcvy_prj clm_rcvy_sit clm_rcvy_stg ecr ecr_allphi_prj ecr_nophi_prj ecr_prj ecr_uat ecr_utlty ext ga_rfp_prj health_wellness health_wellness_prj inv inv_prj ivc ivc_prj ma_apcd ma_apcd_prj ma_apcd_sit ma_apcd_uat menh_apcd menh_apcd_prj migrate ops_rptg_prj optum_prj prov_inq_rpt ris_etg ris_etg_allphi ris_etg_allphi_sit ris_etg_nophi ris_etg_nophi_sit ris_etg_prj ris_etg_sit t1_clm_rcvy t1_clm_rcvy_buss t1_clm_rcvy_stg t1_ma_apcd t1_ris_etg t1_ris_etg_allphi t1_ris_etg_nophi ut_apcd ut_apcd_prj clm_rcvy_stg_prj clm_rcvy_stg_sit clm_rcvy_utlty clm_rcvy_utlty_prj public"
SCHEMAS="clm_rcvy_lz clm_rcvy_lz_prj clm_rcvy_lz_sit clm_rcvy_prj clm_rcvy_sit clm_rcvy_stg clm_rcvy_stg_prj ris_etg_allphi_sit ris_etg_allphi_sit ris_etg_allphi_sit"

for schema in $SCHEMAS
do

    message "dumping schema for $DB: pg_dump -s -n $schema $DB  $ARTIFACT_DIR/${DB}.${schema}.ddl"
    # keep original w/constraints in place
    pg_dump -Fp -s -n $schema $DB  > $ARTIFACT_DIR/${DB}.${schema}_orig.ddl 
    # remove not null constraints for loading
    sed -e 's/NOT NULL//' $ARTIFACT_DIR/${DB}.${schema}_orig.ddl > $ARTIFACT_DIR/${DB}.${schema}.ddl 
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
egrep 'CREATE DATABASE|already exists' /tmp/db_cr.out > /dev/null 2>&1
if [[ $? == 1 ]]; then
    message "error creating database, aborting"
    exit 1
fi
# sql_error $? /tmp/db_cr.out "create db $DB" ABORT

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
cat /tmp/globals_restore.out >> $LOGFILE

# restore ddl by schema
# errors in sql here are recoverable, so continue even if we error
for schema in $SCHEMAS
do
    message "Creating schema/tables on remote $ARTIFACT_DIR/${DB}.${schema}.ddl" /tmp/schema_${schema}_restore.out
    psql -f $ARTIFACT_DIR/${DB}.${schema}.ddl $DB > /tmp/schema_${schema}_restore.out 2>&1 
    sql_error $? /tmp/schema_${schema}_restore.out $ARTIFACT_DIR/${DB}.${schema}.ddl 
    # capture the failed db/schema and file in the redo_log
    if [[ $RET_VAL == 1 ]]; then
        redo_log "$DB $schema restore of $ARTIFACT_DIR/${DB}.${schema}.ddl"
    fi
    cat /tmp/schema_${schema}_restore.out >> $LOGFILE
done

message "$(date) $0 completed successfully"
exit 0
