#!/bin/bash

# Cron Entry: (cpnrs6a/b)
# 25 1,7,13,19 * * * ldm bash -c '/usr/local/ldm/util/getNBM_qpfExceedance_srh.sh  &> /usr/local/ldm/logs/getNBM_qpfExceedance_srh.log'

echo -n "START: "
date

YYYYMMDD=$(date -u +%Y%m%d)
if [ $# -eq 0 ]
then
        hr=$(date -u +%k)
        if [ $hr -lt 5 ]
        then
                YYYYMMDD=$(date -u -d "yesterday" +%Y%m%d)
                cycle="18"
        elif [ $hr -ge 5 -a $hr -lt 10 ]
        then
                cycle="00"
        elif [ $hr -ge 10 -a $hr -lt 17 ]
        then
                cycle="06"
        elif [ $hr -ge 17 ]
        then
                cycle="12"
        else
                echo "No arguments and could not determine cycle date/time.  Exiting..."
                exit 1
        fi
else
        cycle=$1
fi

grib_dir="/data/grid_download/"
out_dir="/data/gribsend"
wgrib="/usr/local/bin/wgrib2"
#domain="-set_grib_type c3 -small_grib -114:-79 33:50"
domain="-set_grib_type c3 -grib"
utilPath="/usr/local/ldm/util/"

cd $grib_dir
#rm -rf *blend.* ${out_dir}/SR-NBM*

getGrib () {
        local fh=$1
        type=$2
        INV_URL="https://nomads.ncep.noaa.gov/pub/data/nccf/com/blend/prod/blend.${YYYYMMDD}/${cycle}/qmd/blend.t${cycle}z.qmd.f${fh}.co.grib2.idx"
        GRIB_URL="https://nomads.ncep.noaa.gov/pub/data/nccf/com/blend/prod/blend.${YYYYMMDD}/${cycle}/qmd/blend.t${cycle}z.qmd.f${fh}.co.grib2"
        echo "INV_URL = $INV_URL"
        echo "GRIB_URL = $GRIB_URL"
        $utilPath/get_inv.pl $INV_URL | egrep '(:APCP:surface:.*:prob >)' | $utilPath/get_grib.pl $GRIB_URL ${grib_dir}/blend.t${cycle}z.qmd.f${fh}.co.grib2
        if [ $? -ne 0 ]
        then
                echo "Forecast hour: $fh not available.  Sleeping 1 minute before trying again."
                echo "Try #${tries}"
                tries=$((tries++))
                if [ $tries -ge 30 ]
                then
                        echo "Failed to retrieve file for $fh hrs"
                        return 1
                fi
                sleep 60
                getGrib $fh $type
        else
                return 0
        fi
}

for fhr in 024 036 048 060 072 084 096 108 120 132 144 156 168 180 192 204
do
        tries=1
        echo "Retrieving NBM prob exceedance for $fhr hrs"
        getGrib $fhr $type
        if [ $? -eq 0 ]
        then
                mv ${grib_dir}/blend.t${cycle}z.qmd.f${fhr}.co.grib2 ${out_dir}/SR-NBM.t${cycle}z.f${fhr}.probQPF.co.grib2

                cd $out_dir
                for i in $(ls -1 SR-NBM.t${cycle}z.f${fhr}.*)
                do
                    if [ -s "$i" ]
                    then
                        echo "sending to LDM"
                        /usr/local/ldm/bin/pqinsert -f EXP $i
                    fi
                done
                cd $grib_dir
                #allow a little time for LDM delivery before going on to next forecast hour
                sleep 5

        fi
done

#rm -rf ${grib_dir}/SR-NBM.*

echo -n "FINISH: "
date


