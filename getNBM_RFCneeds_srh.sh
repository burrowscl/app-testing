#!/bin/bash

# Cron Entry: (cpnrs6a/b)
# 25 2,8,14,20 * * * ldm bash -c '/usr/local/ldm/util/getNBM_RFCneeds_srh.sh  &> /usr/local/ldm/logs/getNBM_RFCneeds_srh.log'

echo -n "START: "
date

YYYYMMDD=$(date -u +%Y%m%d)
if [ $# -eq 0 ]
then
        hr=$(date -u +%k)
        if [ $hr -lt 5 ]
        then
                #YYYYMMDD=$(date -u -d "yesterday" +%Y%m%d)
                cycle="01"
        elif [ $hr -ge 5 -a $hr -lt 10 ]
        then
                cycle="07"
        elif [ $hr -ge 10 -a $hr -lt 17 ]
        then
                cycle="13"
        elif [ $hr -ge 17 ]
        then
                cycle="19"
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
#rm -rf *blend.* ${out_dir}/SR-NBM.*RFCneeds*

getGrib () {
        local fh=$1
        echo ${fh}
        prevfh=$((fh - 6))
        type=$2
        echo `date`
        qpf="(:APCP:surface:${prevfh}-${fh} hour acc fcst:)"
        INV_URL="https://nomads.ncep.noaa.gov/pub/data/nccf/com/blend/prod/blend.${YYYYMMDD}/${cycle}/core/blend.t${cycle}z.core.f${fh}.co.grib2.idx"
        GRIB_URL="https://nomads.ncep.noaa.gov/pub/data/nccf/com/blend/prod/blend.${YYYYMMDD}/${cycle}/core/blend.t${cycle}z.core.f${fh}.co.grib2"
        echo "INV_URL = $INV_URL"
        echo "GRIB_URL = $GRIB_URL"
        $utilPath/get_inv.pl $INV_URL | egrep "$qpf|(:TMP:2 m above ground)|(:SNOWLVL:0 m above mean sea level)|(:TMAX:2 m above ground)|(:TMIN:2 m above ground)" | $utilPath/get_grib.pl $GRIB_URL ${grib_dir}/blend.t${cycle}z.core.f${fh}.co.grib2
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

for fhr in 185 188 191 197 203 209 215 221 227 233 239 245 251 257 263
do
        tries=1
        prevfh=$((fhr - 6))
        echo `date`
        echo "Retrieving NBM data for RFC needs for $fhr hrs"
        getGrib $fhr $type
        if [ $? -eq 0 ]
        then

                $wgrib ${grib_dir}/blend.t${cycle}z.core.f${fhr}.co.grib2 \
                     -not ":APCP:surface:${prevfh}-${fhr} hour acc fcst:prob >0.254:prob fcst" -grib ${out_dir}/SR-NBM.t${cycle}z.f${fhr}.RFCneeds.co.grib2

                cd $out_dir
                for i in $(ls -1 SR-NBM.t${cycle}z.f${fhr}.*)
                do
                    if [ -s "$i" ]
                    then
                        /usr/local/ldm/bin/pqinsert -f EXP $i
                    fi
                done
                cd $grib_dir
                #allow a little time for LDM delivery before going on to next forecast hour
                sleep 5

        fi
done

#rm -rf ${grib_dir}/blend.t*

echo -n "FINISH: "
date


