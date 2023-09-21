#!/bin/bash

echo -n "START: "
date

# 45 1,13 * * * ldm bash -c '/usr/local/ldm/util/get_highresw_srh.sh &> /usr/local/ldm/logs/get_highresw_cron.log'
#

if [ "${LDMHOME}" == "" ]
    then
    export LDMHOME="/usr/local/ldm"
    echo "WARNING - Your LDMHOME var is not set, defaulting to ${LDMHOME}"
fi

source ${LDMHOME}/.bashrc
source ${LDMHOME}/util/process_lock.sh

user=`whoami`
if  [ "$user" != "ldm" ]
    then
    echo "You must be user ldm to run this script"
    exit 1
fi

export TZ=UTC

# Script variables
# ===========================================================
# TODO: Need to change to LDM home
BINdir="${LDMHOME}/util"
LOGdir="${LDMHOME}/logs"
VARdir="${LDMHOME}/var"

# Set our data processing directory
DATAdir="/data"
PRODUCTdir="${DATAdir}/gribsend"
SPOOLdir="${DATAdir}/grid_download"
LOGfile=${LOGdir}/get_highresw_model.log
wgrib="/usr/local/bin/wgrib2"
utilPath="/usr/local/ldm/util"

# Check for command line CYCLE
#if [ -z "$1" ]; then
#    echo "Usage: $0 rdps|gdps [cycle hour]"
#    exit 1;
#fi

#PRODUCT=${1}
#if [ ! -z $2 ]; then CYCLE=$2; fi
#if [ ! -z $3 ]; then HOUR=$3; fi

# Set our locking variables
PROGRAMname="$0"
LOCKfile="$VARdir/get_highresw_${1}.lck"
MINold="60"

LockFileCheck $MINold
CreateLockFile

# Make any of the following directories if needed
mkdir -p ${PRODUCTdir}
mkdir -p ${SPOOLdir}
mkdir -p ${VARdir}
mkdir -p ${LOGdir}

#export OMP_NUM_THREADS=4
YYYYMMDD=$(date -u +%Y%m%d)


#export OMP_NUM_THREADS=4
YYYYMMDD=$(date -u +%Y%m%d)
if [ $# -eq 0 ]
then
    hr=$(date -u +%k)
    if [ $hr -ge 0 -a $hr -lt 1 ]
    then
        YYYYMMDD=$(date -u -d "yesterday" +%Y%m%d)
        cycle="12"
    elif [ $hr -ge 1 -a $hr -lt 13 ]
    then
        cycle="00"
    elif [ $hr -ge 13 -a $hr -le 23 ]
    then
        cycle="12"
    else
        echo "No arguments and could not determine cycle date/time.  Exiting..."
        exit 1
    fi
    modelList="arw fv3"
else
    cycle=$1
    if [ $# -gt 1 ]
    then
        modelList=${@:2}
    fi
fi

#newgridEast="-set_grib_type c3 -new_grid_winds earth -new_grid lambert:-89.0:38.0 -109.8:884:5000.0 22.1:614:5000.0"
#newgridWest="-set_grib_type c3 -new_grid_winds earth -new_grid lambert:-108.0:40.5 -129.2:884:5000.0 24.5:614:5000.0"
newgrid="-set_grib_type c3 -new_grid_winds earth -new_grid lambert:-97.5:38.5 -122.72:1079:5000.0 21.138:635:5000.0"

cd $SPOOLdir
/bin/rm -f conusarw* conusnmmb* ${PRODUCTdir}/SR-arw* ${PRODUCTdir}/SR-nmmb* ${PRODUCTdir}/conusarw* ${PRODUCTdir}/conusnmmb* conus* ${PRODUCTdir}/conus*

getGrib () {
    local fh=$1
    local modelType=$2
    local fhTest=$(echo $fh|sed 's/^0*//')
    #INV_URL="https://www.ftp.ncep.noaa.gov/data/nccf/com/hiresw/prod/hiresw.${YYYYMMDD}/hiresw.t${cycle}z.${modelType}_5km.f${fh}.conus.grib2.idx"
    #GRIB_URL="https://www.ftp.ncep.noaa.gov/data/nccf/com/hiresw/prod/hiresw.${YYYYMMDD}/hiresw.t${cycle}z.${modelType}_5km.f${fh}.conus.grib2"
    INV_URL="https://nomads.ncep.noaa.gov/pub/data/nccf/com/hiresw/prod/hiresw.${YYYYMMDD}/hiresw.t${cycle}z.${modelType}_5km.f${fh}.conus.grib2.idx"
    GRIB_URL="https://nomads.ncep.noaa.gov/pub/data/nccf/com/hiresw/prod/hiresw.${YYYYMMDD}/hiresw.t${cycle}z.${modelType}_5km.f${fh}.conus.grib2"
    echo "INV_URL = $INV_URL"
    echo "GRIB_URL = $GRIB_URL"
    if (( fhTest % 3 ))
    then

        $utilPath/get_inv.pl $INV_URL | egrep '(UGRD:10 m above ground|VGRD:10 m above ground|MSLET:mean sea level|TMP:2 m above ground|DPT:2 m above ground|APCP:surface|REFD:1000 m above ground|REFC:entire atmosphere|CAPE:surface|CIN:surface|VIS:surface|TCDC:entire atmosphere|MAXREF:1000 m above ground|UPHL:5000-2000 m above ground|MXUPHL:5000-2000 m above ground|MAXUVV:400-1000 mb|MAXDVV:400-1000 mb|CRAIN:surface|CFRZR:surface|CICEP:surface|CSNOW:surface|HGT:cloud)' | $utilPath/get_grib.pl $GRIB_URL ${SPOOLdir}/conus${modelType}.t${cycle}z.F${fh}.grib2

    else

        $utilPath/get_inv.pl $INV_URL | egrep '(DPT:2 m above ground|APCP:surface|MAXREF:1000 m above ground|UPHL:5000-2000 m above ground|MXUPHL:5000-2000 m above ground|MAXUVV:400-1000 mb|MAXDVV:400-1000 mb)' | $utilPath/get_grib.pl $GRIB_URL ${SPOOLdir}/conus${modelType}.t${cycle}z.F${fh}.grib2

    fi
    if [ $? -ne 0 ]
    then
        tries=$((tries++))
        if [ $tries -ge 30 ]
        then
            echo "Failed to retrieve file for $fh hrs. Waiting 2 minutes before trying again."
            return 1
        fi
        sleep 60
        getGrib $fh $modelType
    fi

}

getGrib2 () {
    local fh=$1
    local modelType=$2
    local fhTest=$(echo $fh|sed 's/^0*//')
    #INV_URL="https://www.ftp.ncep.noaa.gov/data/nccf/com/hiresw/prod/hiresw.${YYYYMMDD}/hiresw.t${cycle}z.${modelType}_5km.f${fh}.conus.grib2.idx"
    #GRIB_URL="https://www.ftp.ncep.noaa.gov/data/nccf/com/hiresw/prod/hiresw.${YYYYMMDD}/hiresw.t${cycle}z.${modelType}_5km.f${fh}.conus.grib2"
    INV_URL="https://nomads.ncep.noaa.gov/pub/data/nccf/com/hiresw/prod/hiresw.${YYYYMMDD}/hiresw.t${cycle}z.${modelType}_5km.f${fh}.conus.grib2.idx"
    GRIB_URL="https://nomads.ncep.noaa.gov/pub/data/nccf/com/hiresw/prod/hiresw.${YYYYMMDD}/hiresw.t${cycle}z.${modelType}_5km.f${fh}.conus.grib2"
    echo "INV_URL = $INV_URL"
    echo "GRIB_URL = $GRIB_URL"
    $utilPath/get_inv.pl $INV_URL | egrep '(UGRD:10 m above ground|VGRD:10 m above ground|MSLET:mean sea level|TMP:2 m above ground|DPT:2 m above ground|APCP:surface|REFD:1000 m above ground|REFC:entire atmosphere|CAPE:surface|CIN:surface|VIS:surface|TCDC:entire atmosphere|MAXREF:1000 m above ground|UPHL:5000-2000 m above ground|MXUPHL:5000-2000 m above ground|MAXUVV:400-1000 mb|MAXDVV:400-1000 mb|CRAIN:surface|CFRZR:surface|CICEP:surface|CSNOW:surface|HGT:cloud)' | $utilPath/get_grib.pl $GRIB_URL ${SPOOLdir}/conus${modelType}.t${cycle}z.F${fh}.grib2
    if [ $? -ne 0 ]
    then
        tries=$((tries++))
        if [ $tries -ge 30 ]
        then
            echo "Failed to retrieve file for $fh hrs. Waiting 2 minutes before trying again."
            return 1
        fi
        sleep 60
        getGrib $fh $modelType
    fi

}

for fhr in $(seq -w 1 48)
do
    for modType in $modelList
    do
        tries=0
        echo "Retrieving CONUS $modType for $fhr hrs"
        getGrib $fhr $modType
        if [ $? -eq 0 ]
        then
            # Convert domain
            echo "Running wgrib2 to change domain from NOMADS to SBN size"
            $wgrib ${SPOOLdir}/conus${modType}.t${cycle}z.F${fhr}.grib2 $newgrid ${PRODUCTdir}/conus${modType}.t${cycle}z.F${fhr}.grib2

            cd $PRODUCTdir
            for i in $(ls -1 conus${modType}.t${cycle}z.F${fhr}.*)
            do
                if [ -s "$i" ]
                then
                    echo "ldm send ${i}"
                    /usr/local/ldm/bin/pqinsert -f EXP $i
                fi
            done
            cd $SPOOLdir
        fi
    done
done

for fhr in $(seq -w 49 60)
do
        tries=0
        echo "Retrieving CONUS fv3 for $fhr hrs"
        getGrib2 $fhr fv3
        if [ $? -eq 0 ]
        then
            # Convert domain
            echo "Running wgrib2 to change domain from NOMADS to SBN size"
            $wgrib ${SPOOLdir}/conusfv3.t${cycle}z.F${fhr}.grib2 $newgrid ${PRODUCTdir}/conusfv3.t${cycle}z.F${fhr}.grib2

            cd $PRODUCTdir
            for i in $(ls -1 conusfv3.t${cycle}z.F${fhr}.*)
            do
                if [ -s "$i" ]
                then
                    echo "ldm send ${i}"
                    /usr/local/ldm/bin/pqinsert -f EXP $i
                fi
            done
            cd $SPOOLdir
        fi
done

cd $SPOOLdir
rm -f conus* ${PRODUCTdir}/conus*


