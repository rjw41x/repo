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

# select *, pg_size_pretty( dfspace ) from gp_toolkit.gp_disk_free; #  check disk free space

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

message $(date) $0 start PGHOST: $PGHOST

# get list of schemas
# SCHEMAS=$(psql -t -c "select schema_name from information_schema.schemata where schema_name not in ( 'gp_toolkit', 'pg_toast', 'pg_bitmapindex', 'pg_aoseg', 'pg_catalog', 'information_schema', 'ext', 'madlib');" $DB )
# SCHEMAS="clm_rcvy_buss_sit clm_rcvy_lz clm_rcvy_lz_prj clm_rcvy_lz_sit clm_rcvy_prj clm_rcvy_sit clm_rcvy_stg cms co_apcd_prj co_apcd_uat csts ct_apcd ct_apcd_prj ecr ecr_allphi_prj ecr_nophi_prj ecr_prj ecr_uat ecr_utlty ext ga_rfp_prj health_wellness health_wellness_prj inv inv_prj ivc ivc_prj ma_apcd ma_apcd_prj ma_apcd_sit ma_apcd_uat menh_apcd menh_apcd_prj migrate ops_rptg_prj optum_prj prov_inq_rpt ris_etg ris_etg_allphi ris_etg_allphi_sit ris_etg_nophi ris_etg_nophi_sit ris_etg_prj ris_etg_sit t1_clm_rcvy t1_clm_rcvy_buss t1_clm_rcvy_lz t1_clm_rcvy_stg t1_ma_apcd t1_ris_etg t1_ris_etg_allphi t1_ris_etg_nophi ut_apcd ut_apcd_prj clm_rcvy_stg_prj clm_rcvy_stg_sit clm_rcvy_utlty clm_rcvy_utlty_prj public clm_rcvy_stg_prj clm_rcvy_stg_sit clm_rcvy_utlty clm_rcvy_utlty_prj public ris_etg_allphi_sit ris_etg_allphi_sit ris_etg_allphi_sit "
SCHEMAS="clm_rcvy_stg_prj"
# 
# public va_apcd va_apcd_prj vt_apcd vt_apcd_prj vt_apcd_prj"
message proceeding with schemas: $SCHEMAS

# create schema ext ignore error if it exists
psql -d $DB -c "create schema ext;" > /dev/null 2>&1

# MAIN
for schema in $SCHEMAS
do
    # check to see that gpfdist procs are running
    # ./manage_gpfdist.sh restart #  restart gpfdists between schemas
    ./manage_gpfdist.sh check > /dev/null 2>&1
    if [[ $? != 0 ]]; then
        message "gpfdist processes are not running properly, restart failed, exiting"
        exit 1
    fi
    message "processing schema $schema"
    # select relname from pg_exttable x, pg_class c where x.reloid = c.oid limit 5;
    # select tablename from pg_tables t, pg_class c, pg_exttable x where schemaname = 'bball' AND c.oid = x.reloid and c.relname = t.tablename;
    # select relname, relstorage from pg_class c, pg_exttable x where c.oid = x.reloid 

    # order by should make partition parent tables appear first
    TABLES=$(psql -d $DB -t -c "select table_name from information_schema.tables where table_schema = '$schema' and table_type <> 'VIEW' order by table_name;")
    message STARTING table list: $TABLES
    if [[ -z "$SKIP_EXT" ]]; then
 
         > /tmp/partitions_
         for table in $TABLES
         do
             # check to see if table has partitions
             grep "${table}$" /tmp/partitions_ > /dev/null 2>&1
             # if we find the table in the partitions list there is no need to continue with it
             if [[ $? == 0 ]]; then
                 continue
             fi
             # check to see if table has partitions
             cnt=$(psql -t -c "select count(*) from pg_partitions where tablename = '${table}' group by tablename;" | awk 'NR == 1 {printf("%d",$1);}')
             if [[ $cnt -gt 0 ]]; then
                 part_tbls=$(psql -t -c "select distinct partitiontablename from pg_partitions where tablename='${table}';" $DB)
                 # remove partitions from copy - base table will move all of the rows
                 for partition in $part_tbls
                 do
                     echo $partition >> /tmp/partitions_
                     TABLES=$(echo $TABLES | sed -e "s/$partition//")
                 done
             fi
         done
        # CREATE EXT TABLES
        for table in $TABLES
        do
            ONE_FILE=false
            # determine if table is an external table and skip it
            # RJW - Old query - 05/08/17
            # psql -d $DB -t -c "select reloid, relname from pg_exttable x, pg_class c where x.reloid = c. oid and relname = '$table';" > /tmp/$$.out 2>&1
            # RJW - NEW query - 05/08/17
            # link schema, class (tables++) and exttable - rows that return (non empty) ARE external tables
            psql -d $DB -t -c "SELECT relname, relstorage \
            FROM pg_class c, pg_namespace n, pg_exttable x \
            WHERE n.nspname = '$schema' and c.relname = '$table' and x.reloid = c.oid AND n.oid = c.relnamespace ;" > /tmp/$$.out 2>&1
            sql_ext /tmp/$$.out $table
            if [[ $EMPTY == 'NO' ]]; then
                message TABLE $table is an external table, skipping
                # remove table from list so it doesn't get processed during unload
                TABLES=$(echo $TABLES | sed -e "s/$table//")
                # skip this table (external)
                continue
            fi
            rows=$(psql -t -c "SELECT count(1) from ${schema}.${table};" $DB | awk 'NR == 1 { printf("%d",$1); }')
            echo $schema $table $rows
            if [[ $rows == 0 ]]; then
                message ${schema}.${table} is empty, skipping unload
                TABLES=$(echo $TABLES | sed -e "s/$table//")
                continue
            elif [[ $rows -lt 10000 ]]; then
                ONE_FILE=true
            fi
            psql -d $DB -c "drop external table if exists ext.${table};" > /dev/null 2>&1
            echo "create writable external table ext.$table ( like ${schema}.${table} )
            location (" > /tmp/$$cr_ext.sql
            cnt=0
            num_segs=$(echo $NEW_SEGS | wc -w)
            for seg in $NEW_SEGS
            do
                if [[ $ONE_FILE == 'true' ]]; then
                    message $schema $table has $rows rows and is ONE FILE
                    echo "'gpfdist://${seg}:${WRITE_PORT1}/${schema}.${table}.psv','gpfdist://${seg}:${WRITE_PORT2}/${schema}.${table}.psv'" >> /tmp/$$cr_ext.sql 
                    touch /tmp/${schema}.${table}.ONE_FILE
                    break
                fi
                cnt=$((cnt+1))
                if [[ $num_segs == $cnt ]]; then
                    echo "'gpfdist://${seg}:${WRITE_PORT1}/${schema}.${table}.psv','gpfdist://${seg}:${WRITE_PORT2}/${schema}.${table}.psv'" >> /tmp/$$cr_ext.sql 
                else
                    echo "'gpfdist://${seg}:${WRITE_PORT1}/${schema}.${table}.psv','gpfdist://${seg}:${WRITE_PORT2}/${schema}.${table}.psv'," >> /tmp/$$cr_ext.sql 
                fi
                    
            done
            echo ") format 'text' ( delimiter '|' null ''  ) distributed randomly;" >> /tmp/$$cr_ext.sql
            # create the table, trapping any errors
            psql -d $DB -f /tmp/$$cr_ext.sql > /tmp/$$cr_ext.out 2>&1 
            sql_error $? /tmp/$$cr_ext.out /tmp/$$cr_ext.sql 
            if [[ $RET_VAL != 0 ]]; then
                message "FAIL: external table ext.$table $(cat /tmp/$$cr_ext.out /tmp/$$cr_ext.sql)"
                # add the table to the redo log on failure
                redo_log $table 
                continue # goto next table
            else
                log "SUCCESS: external table ext.$table"
            fi

            # do we want to do anything else in this script?
        done
        # clean up between schemas
        rm /tmp/$$cr_ext.sql /tmp/$$cr_ext.out > /dev/null 2>&1

    # end if SKIP_EXT
    fi
    message ENDING table list: $TABLES
    if [[ -f /tmp/hold ]]; then
        read x
    fi

    # UNLOAD TO EXT TABLES
    for table in $TABLES
    do
        message unloading table $table
        echo "set gp_external_max_segs to 32; insert into ext.$table select * from ${schema}.${table};" > /tmp/$$unload.sql
        psql -d $DB -f /tmp/$$unload.sql > /tmp/$$unload.out 2>&1
        grep 'INSERT' /tmp/$$unload.out > /dev/null 2>&1
        if [[ $? == 0 ]]; then
            message "SUCCESS: table ${schema}.${table} unload $(head -1 /tmp/$$unload.out)"
        else
            message "FAIL: table ${schema}.${table} unload $(cat /tmp/$$unload.out)"
            redo_log ${schema}.${table} unload failed
            continue
        fi
        # build func to set remote semaphore
        # for now set local semaphore
        echo $rows > /tmp/${schema}.${table}.rdy
        # exit 1
    done

    # clean up
    rm /tmp/$$cr_ext.sql /tmp/$$cr_ext.out > /dev/null 2>&1
    rm /tmp/$$unload.sql /tmp/$$unload.out > /dev/null 2>&1

    # pass along final table list to load program
    echo $TABLES > /tmp/${schema}.tables
    # clear variable for next iteration
    TABLES=""
done
# touch /tmp/complete
message $(date) $0 complete
