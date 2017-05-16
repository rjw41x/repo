#!/bin/bash
# DEBUG
# set -x

# pull in master config information
source master_conf.sh

# GLOBALS
LOGFILE=~/logs/gpfdist_start.out
RET_VAL=0

# FUNCTIONS
usage()  {
    echo "$0 start|stop|make"
    echo "    start = start gpdfdist processes
    stop = stop running gpfdist processes
    restart = stop and start gpfdist processes
    check/status = check gpfdist processes
    make = make data directories for the transfer process"
    exit 1
}

# make transfer directories
make_dirs() {
    for host in $NEW_SEGS
    do
        ssh gpadmin@$host "mkdir $DATA_DIR1 $DATA_DIR2"
        if [[ $? != 0 ]]; then
            message "FAIL: directory create $DATA_DIR1 $DATA_DIR2"
            exit 1
        else
            message "SUCCESS: directory create $DATA_DIR1 $DATA_DIR2"
            RET_VAL=0
        fi
    done
}

check() {
    RET_VAL=""
    tgt_gpfd=$( grep 'PORT[1-2]' master_conf.sh | grep -v grep | wc -l)
    num_segs=$(echo $NEW_SEGS | wc -w)
    host_cnt=0
    for host in $NEW_SEGS
    do
        num_gpfd=$(ssh gpadmin@$host ps -ef 2> /dev/null | grep gpfdist | grep -v grep | wc -l)
        host_cnt=$((host_cnt+1))
        if [[ $tgt_gpfd != $num_gpfd ]]; then
            RET_VAL=1
            message "incorrect number of gpfdist running on host $host, try manage_gpfdist.sh restart"
            # subtract host when we end up here
            host_cnt=$((host_cnt-1))
        fi
    done
    if [[ $num_segs == $host_cnt ]]; then
        message "gpfdist running correctly on all hosts"
        RET_VAL=0
    fi
}

start_gpfd() {
    for host in $NEW_SEGS
    do
        # gpfdist for writable ext tables
        ssh gpadmin@$host "source /usr/local/greenplum-db/greenplum_path.sh ; gpfdist -d $DATA_DIR1 -p $WRITE_PORT1 -l $GPFDLOGFILE &" >> $LOGFILE 2>&1 &
sleep 1
        ssh gpadmin@$host "source /usr/local/greenplum-db/greenplum_path.sh; gpfdist -d $DATA_DIR2 -p $WRITE_PORT2 -l $GPFDLOGFILE &" >> $LOGFILE 2>&1 &
sleep 1
        # gpfdist for readable ext tables
        ssh gpadmin@$host "source /usr/local/greenplum-db/greenplum_path.sh; gpfdist -d $DATA_DIR1 -p $READ_PORT1 -l $GPFDLOGFILE &" >> $LOGFILE 2>&1 &
sleep 1
        ssh gpadmin@$host "source /usr/local/greenplum-db/greenplum_path.sh; gpfdist -d $DATA_DIR2 -p $READ_PORT2 -l $GPFDLOGFILE &" >> $LOGFILE 2>&1 &
sleep 2
    done

     sleep 2
     check
     if [[ $RET_VAL == 0 ]]; then
         # made it to here so all is good
         message "SUCCESS: gpdfist processes started "
         RET_VAL=0
     else
         message "FAIL:  gpdfist processes did not start properly "
         RET_VAL=1
     fi
}

stop_gpfd() {
    for host in $NEW_SEGS
    do
        # ssh gpadmin@$host "for proc in $(ps -ef 2> /dev/null | grep gpfdist | grep -v grep | awk '{ printf("%s ",$2);}'); do kill \$proc; done " > /dev/null 2>&1
        ssh gpadmin@$host ps -ef 2> /dev/null | grep gpfdist | grep -v grep| awk '{ printf("%s ",$2);}' > /tmp/$$procs
        for process in $(cat /tmp/$$procs)
        do
            ssh gpadmin@$host kill $process > /dev/null 2>&1
        done
        # num_gpfd=$(ssh gpadmin@$host "echo $(ps -ef 2> /dev/null | grep gpfdist | grep -v grep | wc -l)")
        num_gpfd=$(ssh gpadmin@$host ps -ef 2> /dev/null | grep gpfdist | grep -v grep | wc -l)
        if [[ $num_gpfd == 0 ]]; then
            message "gpfdist processes on host $host have been killed"
            RET_VAL=0
        else
            message "Not all gpfdist shutdown on host $host"
            RET_VAL=1
        fi
    done
    rm /tmp/$$procs
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

    "check"|"status" )
        check
    ;;

    "restart" )
        stop_gpfd
        start_gpfd
    ;;

    "make" )
        make_dirs
    ;;

    * )
    usage
    ;;
esac
exit $RET_VAL
