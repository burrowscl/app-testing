#!/bin/bash

if [ "${LDMHOME}" == "" ]
    then
    export LDMHOME="/usr/local/ldm"
    echo "WARNING - Your LDMHOME var is not set, defaulting to ${LDMHOME}"
fi

source ${LDMHOME}/.bashrc

user=`whoami`
hostname=`hostname -s`

if  [ "$user" != "ldm" ]  
    then
    echo "You must be user ldm to run this script"
    exit 1

fi

source /usr/local/ldm/util/process_lock.sh

PROGRAMname="$0"
VARdir="/usr/local/ldm/var"
MINold="5"
LOCKfile="${VARdir}/mpingnwsldm.lck"
LOGfile="/usr/local/ldm/logs/mpingnws.log"

LockFileCheck $MINold
CreateLockFile

mkdir -p /data.local/mping

cd /usr/local/ldm
/usr/local/ldm/util/mpingldm/mpingnws_srhldm.py

RemoveLockFile
exit 0


