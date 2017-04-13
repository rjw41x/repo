#!/bin/bash

# pull in master config information
source master_conf.sh

# GLOBALS
LOGFILE=~/logs/gpfdist_start.out

# FUNCTIONS
usage()  {
    echo "$0 start|stop|make"
    echo "    start = start gpdfdist processes
    stop = stop running gpfdist processes
    make = make data directories for the transfer process"
    exit 1
}
check_status() {
    if [[ $1 == 0 ]]; then
        return 0
    else
        message "failed to start gpfdist $2, aborting.  Check logs $LOGFILE"
        exit 1
    fi
}

# make transfer directories
make_dirs() {
    for host in $NEW_SEGS
    do
        ssh gpadmin@$host "mkdir $DATA_DIR1 $DATA_DIR2"
        if [[ $? != 0 ]]; then
            message "FAIL: directory create $DATA_DIR1 $DATA_DIR2"
        else
            message "SUCCESS: directory create $DATA_DIR1 $DATA_DIR2"
        fi
    done
}
start_gpfd() {
    for host in $NEW_SEGS
    do
        # gpfdist for writable ext tables
        ssh gpadmin@$host "source /usr/local/greenplum-db/greenplum_path.sh ; gpfdist -d $DATA_DIR1 -p $WRITE_PORT1 -l $GPFDLOGFILE &" >> $LOGFILE 2>&1 &
        check_status $? writable1
        ssh gpadmin@$host "source /usr/local/greenplum-db/greenplum_path.sh; gpfdist -d $DATA_DIR2 -p $WRITE_PORT2 -l $GPFDLOGFILE &" >> $LOGFILE 2>&1 &
        check_status $? writable2
        # gpfdist for readable ext tables
        ssh gpadmin@$host "source /usr/local/greenplum-db/greenplum_path.sh; gpfdist -d $DATA_DIR1 -p $READ_PORT1 -l $GPFDLOGFILE &" >> $LOGFILE 2>&1 &
        check_status $? readable1
        ssh gpadmin@$host "source /usr/local/greenplum-db/greenplum_path.sh; gpfdist -d $DATA_DIR2 -p $READ_PORT2 -l $GPFDLOGFILE &" >> $LOGFILE 2>&1 &
        check_status $? readable2
    done

    # made it to here so all is good
    message "gpdfist processes started successfully"
}

stop_gpfd() {
    for host in $NEW_SEGS
    do
        ssh gpadmin@$host "for proc in $(ps -ef | grep gpfdist | grep -v grep | awk '{ printf("%s ",$2);}'); do kill \$proc; done " > /dev/null 2>&1
        num_gpfd=$(ssh gpadmin@rjw1 "echo $(ps -ef | grep gpfdist | grep -v grep | wc -l)")
        if [[ $num_gpfd == 0 ]]; then
            message "gpfdist processes on host $host have been killed"
        else
            message "Not all gpfdist shutdown on host $host"
        fi
    done
}

# make sure we are gpadmin
if [[ $USER != "gpadmin" ]]; then
    message "gpfdist must be started as gpadmin - aborting"
    usage
    exit 1
fi
if [[ -z "$1" ]]; then
    usage
fi

case $1 in
    "start" )
        start_gpfd
    ;;

    "stop" )
        stop_gpfd
    ;;

    "make" )
        make_dirs
    ;;

    * )
    usage
    ;;
esac