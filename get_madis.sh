#!/bin/bash

#########################################################################
# AUTHOR: tony.freeman@noaa.gov (2 Nov 2015)
# VERSION: 1.0
# * This file is set up specifically for SR.
# * If other regions want I can configure it more generically.
# * Need to provide USER and PASS information for MADIS access.
#
# Updated: 23 May 2018 (tony.freeman@noaa.gov)
# * Fixed issue of LDADinfo.txt and LdadPatterns and a number of junk
#   txt files introduced by MADIS on May 14.  The Stations tar.gz file
#   will now only contain *Station.txt and *.desc files.
# 
#########################################################################

USER="EHU3_madis_wfo"
PASS="p_7TaPurV9HW"

#########################################################################

function ldm_send () {
	for i in 1 2
	do
		if scp -q $1 ldm@srh-ls-cpnrs${i}.srh.noaa.gov:/tmp/
		then
			ssh -q ldm@srh-ls-cpnrs${i}.srh.noaa.gov "cd /tmp; /usr/local/ldm/bin/pqinsert -v -f EXP $1"
			ssh -q ldm@srh-ls-cpnrs${i}.srh.noaa.gov "rm -f /tmp/$1"
		fi
	done
	rm $1
}

#########################################################################

DATESTAMP=$(date +%Y%m%d)

test -d /usr/local/ldm/util/Madis.Station.Files/MADIS || mkdir -p /usr/local/ldm/util/Madis.Station.Files/MADIS

cd /usr/local/ldm/util/Madis.Station.Files/MADIS/

#########################################################################
# Send out the Scripts file before doing the Stations file:
#########################################################################

wget -A pl --http-user=${USER} --http-password=${PASS} --no-check-certificate -c -np -nd -r -l 1 -p https://madis-data.ncep.noaa.gov/madisWfo/wfo/scripts/

find . -size 0b -delete

for i in *.pl
do
	sed -i 's#/usr/local/perl/bin/perl#/usr/bin/perl#g' $i
	sed -i 's#/usr/local/bin/perl#/usr/bin/perl#g' $i
done

touch *

chmod 775 *.??

mv build_station_idx.pl build_station_idx.pl.madis-new

# fix to cover CONUS as default and run on px machines:
sed -r -i -e 's#/ldad/madisfilter#/awips/ldad/data#g' -e 's/110.0/133.0/g' -e 's/41.0/48.0/g' -e 's/102.0/68.0/g' -e 's/37.0/20.5/g' build_station_idx.pl.madis-new

tar --remove-files -cf Madis_Scripts_${DATESTAMP}.tar *.pl pre*.sh *.madis-new 2>/dev/null

gzip Madis_Scripts_${DATESTAMP}.tar

ldm_send Madis_Scripts_${DATESTAMP}.tar.gz

#########################################################################
# Wait for Scripts to get sent and processed then send Stations:
#########################################################################

sleep 10 

mkdir blah.$$
cd blah.$$

wget -A txt,desc --http-user=${USER} --http-password=${PASS} --no-check-certificate -c -np -nd -r -l 1 -p https://madis-data.ncep.noaa.gov/madisWfo/wfo/LDAD/tables/

find . -size 0b -delete

touch *

chmod ug+rw *

rm -f CQC-*
rm -f RW-*

mv *.desc ../
mv *Station.txt ../

cd ..
rm -rf blah.$$

tar --remove-files -cf Madis_Stations_${DATESTAMP}.tar *.txt *.desc

gzip Madis_Stations_${DATESTAMP}.tar

ldm_send Madis_Stations_${DATESTAMP}.tar.gz

#########################################################################

