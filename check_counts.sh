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
LOGFILE=~/logs/count.out

rpt () {
    echo $* >> $DATFILE
}
# get all of the environment information
source master_conf.sh

# get list of schemas
SCHEMAS=$(psql -h $OLD_MASTER -p $OLD_PORT -t -c "select schema_name from information_schema.schemata where schema_name not in ( 'gp_toolkit', 'pg_toast', 'pg_bitmapindex', 'pg_aoseg', 'pg_catalog', 'information_schema', 'ext', 'madlib');" $DB )
# SCHEMAS="clm_rcvy_stg_prj clm_rcvy_buss_sit clm_rcvy_lz clm_rcvy_lz_prj clm_rcvy_lz_sit clm_rcvy_prj clm_rcvy_sitclm_rcvy_stg "
# SCHEMAS="clm_rcvy_buss_sit clm_rcvy_lz clm_rcvy_lz_prj clm_rcvy_lz_sit clm_rcvy_prj clm_rcvy_sit clm_rcvy_stg cms co_apcd_prj co_apcd_uat csts ct_apcd ct_apcd_prj ecr ecr_allphi_prj ecr_nophi_prj ecr_prj ecr_uat ecr_utlty ext ga_rfp_prj health_wellness health_wellness_prj inv inv_prj ivc ivc_prj ma_apcd ma_apcd_prj ma_apcd_sit ma_apcd_uat menh_apcd menh_apcd_prj migrate ops_rptg_prj optum_prj prov_inq_rpt ris_etg ris_etg_allphi ris_etg_allphi_sit ris_etg_nophi ris_etg_nophi_sit ris_etg_prj ris_etg_sit t1_clm_rcvy t1_clm_rcvy_buss t1_clm_rcvy_lz t1_clm_rcvy_stg t1_ma_apcd t1_ris_etg t1_ris_etg_allphi t1_ris_etg_nophi ut_apcd ut_apcd_prj clm_rcvy_stg_prj clm_rcvy_stg_sit clm_rcvy_utlty clm_rcvy_utlty_prj public"
SCHEMAS="clm_rcvy_lz clm_rcvy_lz_prj clm_rcvy_lz_sit clm_rcvy_prj clm_rcvy_sit clm_rcvy_stg clm_rcvy_stg_prj ris_etg_allphi_sit ris_etg_allphi_sit ris_etg_allphi_sit"
SCHEMAS="
message proceeding with schemas: $SCHEMAS

# MAIN
for schema in $SCHEMAS
do
DATFILE=~/logs/${schema}_count.dat
RPTFILE=~/logs/${schema}_count.rpt
> $RPTFILE

    TABLES=$(psql -d $DB -t -c "select table_name from information_schema.tables where table_schema = '$schema';")
    if [[ -z $TABLES ]]; then
        message No tables in schema $schema
        continue
    fi
    message STARTING table list: $TABLES
    for table in $TABLES
    do
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
        old_count=$(psql -h $OLD_MASTER -p $OLD_PORT -t -c "select count(1) from $schema.$table;" $DB |  awk 'NR == 1 { printf("%d",$1); }')
        new_count=$(psql -h $NEW_MASTER -p $NEW_PORT -t -c "select count(1) from $schema.$table;" $DB |  awk 'NR == 1 { printf("%d",$1); }')
        rpt $schema:$table:old:$old_count:new:$new_count
    done
    awk -F":" -v rpt_title="Schema $schema Count Report" 'BEGIN { printf("%s\n", rpt_title ); err=0; } { if( NF == 6 ) if( $4 != $6 ) { printf("Error: %s.%s count mismatch old %d new %d\n",$1,$2,$4,$6); err++; } } END { if( err == 0 ) printf("Success - all table counts match\n"); }' $DATFILE > $RPTFILE

done
