#!/opt/anaconda/bin/python

import os, shutil, sys, subprocess
from datetime import datetime
from multiprocessing.pool import ThreadPool
import pygrib
import requests
from requests.packages.urllib3.util import Retry
from requests.adapters import HTTPAdapter

startTime = datetime.now()

######################################################
#
# CONFIG
#
######################################################
download_dir = '/data/grid_download'  # Where GRIB files will be downloaded
output_dir = '/data/gribsend'         # Where output from wgrib2 will be written
wgrib2 = '/usr/local/bin/wgrib2'      # Full path to wgrib2 binary
pqinsert = '/usr/local/ldm/bin/pqinsert'

if len(sys.argv) < 2:
    print "Usage: {} rdps|gdps [cycle hour]".format(sys.argv[0])
    sys.exit(1)
    
GRID_TYPE = sys.argv[1].upper()

clipBounds = {'RDPS': {'CR': {'clipType': 'latlon',
                              'minX': -114,
                              'maxX': -79,
                              'minY': 33,
                              'maxY': 50},
                       'ER': {'clipType': 'ij',
                              'minX': 150,
                              'maxX': 925,
                              'minY': 1,
                              'maxY': 500},
                       'SR': {'clipType': 'latlon',
                              'minX': -116,
                              'maxX': -75,
                              'minY': 23,
                              'maxY': 40},
                       'WR': {'clipType': 'latlon',
                              'minX': -140,
                              'maxX': -100,
                              'minY': 25,
                              'maxY': 60}
                       },
              'GDPS': {'CR': {'clipType': 'latlon',
                             'minX': -114,
                             'maxX': -79,
                             'minY': 33,
                             'maxY': 50},
                       'ER': {'clipType': 'ij',
                              'minX': 187,
                              'maxX': 542,
                              'minY': 458,
                              'maxY': 625},
                       'SR': {'clipType': 'latlon',
                              'minX': -116,
                              'maxX': -75,
                              'minY': 23,
                              'maxY': 40},
                       'WR': {'clipType': 'latlon',
                              'minX': -140,
                              'maxX': -100,
                              'minY': 25,
                              'maxY': 60}
                       }
                }

regionsToRun = ['SR']

# Variable names to get
vars = ['TMP','DEPR','HGT','UGRD','VGRD','DPT','PRMSL','APCP','TCDC','ABSV','CWAT','PRES']

# List of pressure levels to get variables. Special levels are set in if/else statements below.
all_plevs = [1015,1000,970,950,925,900,875,850,800,750,700,650,600,550,500,400,300,250]

# Levels after hour 168. CMC reduces the amount of data available after hour 168 so we need to change what
# is requested after this hour.
afterHour = 168
after_plevs = [1000,925,850,700,500,400,300,250]

# All variables are subset to an area over the specified region. For fields
# that we want for the entire Northern Hemisphere they need to be entered here.
# The variable name followed by the level. These all have to be in the above 
# configuration. This dictionary is checked when downloading to see what 
# subsetting is needed, it isn't used to list what is downloaded.
nhVars = {
          'ABSV':[500],
          'APCP':[0],
          'CWAT':[0],
          'HGT':[250,500,1000],
          'PRMSL':[0],
          'UGRD':[250],
          'VGRD':[250]
}
nhbounds = '-180.:179.76 0.:89.76'

# What hour to start and stop and what time step to use.
fhourmin = 0 
GDPS_fhourmax = 240
GDPS_fhourstep = 3
GDPS_fhourmaxShort = 84  # How many hours to get 3-hr data. Set to -1 to disable
RDPS_fhourmax = 48
RDPS_fhourstep = 3

# force quit X hours after init time
timeout = 8

# Number of threads to use
NUM_WORKERS = 8

cleanOldFiles = True
######################################################
#
# END CONFIG
#
######################################################

pool = ThreadPool(NUM_WORKERS)

def get_session(urlbase):
    sess = requests.Session()
    ret = Retry(total=20, backoff_factor=60, status_forcelist=[404])
    ret.BACKOFF_MAX = 600
    sess.mount(urlbase, HTTPAdapter(max_retries=ret))
    return sess

def requestWorker(args):
#    print "filebase = ", filebase
#    print "This is {} with {}, {}, {}, {}".format(multiprocessing.current_process().name, args[0], args[1], args[2], args[3])
#    time.sleep(2)
#    print "{} is done.".format(multiprocessing.current_process().name)
    url, localfile, outfile = args
    resp = sess.get(url)
    if resp.status_code != 200:
        print "Error code {} retrieving {}.  Continuing..."
        return None
    elapsed = resp.elapsed.seconds + (resp.elapsed.microseconds / 10.0**6)
    print "Throughput: {:.1f} Mbps".format(len(resp.content) / elapsed * 8.0**-6)
    print "Writing to disk..."
    sys.stdout.flush()
    with open(localfile, 'wb') as fh:
        fh.write(resp.content)
    return (localfile, outfile)

def wgribWorker(args):
    outfile = args[-1]
    try:
        subprocess.check_output(args)
        return outfile
    except:
        return None

def get_gdps():
    # CMC directory that holds the model data
    
    if len(sys.argv) < 3:
        if startTime.hour > 3 and startTime.hour < 16:
            cycle = 0
        else:
            cycle = 12
    else:
        cycle = int(sys.argv[2])
        
    cycleDT = datetime(startTime.year, startTime.month, startTime.day, cycle)
    cycleStr = cycleDT.strftime('%Y%m%d%H')
    print "Running for cycle: {}".format(cycleStr)
    # If running from cron, enable giveup so this doesn't run forever
    if os.isatty(sys.stdin.fileno()):
        giveup = False
    else:
        giveup = True
    
    if cleanOldFiles:
        print 'Removing old CMC_glb files...'
        for dirpath, dirnames, filenames in os.walk(download_dir):
            for file in filenames:
                if "CMC_glb" not in file:
                    continue
                curpath = os.path.join(dirpath, file)
                file_modified = datetime.fromtimestamp(os.path.getmtime(curpath))
                diff = startTime - file_modified
                if diff.days >= 1:
                    #print curpath
                    os.remove(curpath)
         
        print 'Removing old regional clip files...'
        for dirpath, dirnames, filenames in os.walk(output_dir):
            for file in filenames:
                if "CMC25" not in file:
                    continue
                curpath = os.path.join(dirpath, file)
                file_modified = datetime.fromtimestamp(os.path.getmtime(curpath))
                diff = startTime - file_modified
                if diff.days >= 1:
                    #print curpath
                    os.remove(curpath)
    
    levs = {}
    after_levs = {}
    for var in vars:
        if var in ['DEPR','HGT']:
            levs[var] = all_plevs
            after_levs[var] = after_plevs
        if var in ['TMP']:
            levs[var] = all_plevs + [2]
            after_levs[var] = after_plevs + [2]
        if var in ['UGRD','VGRD']:
            levs[var] = all_plevs + [10]
            after_levs[var] = after_plevs + [10]
        if var in ['ABSV']:
            levs[var] = [500]
            after_levs[var] = [500]
        if var in ['PRMSL','APCP','TCDC','CAPE','4LFTX','CWAT','PRES']:
            levs[var] = [0]
            after_levs[var] = [0]
        if var in ['DPT']:
            levs[var] = [2]
            after_levs[var] = [2]

    for fhour in fhours:
        print "fhour: {}".format(fhour)
        # After GDPS_fhourmaxShort, only get 6-hr data
        fullWebDir = '{}/{:02d}/{:03d}'.format(URL_BASE, cycle, fhour)
    
        fileList = []
        TASKS = []
        wgrib_tasks = []
        missingfiles = 0
    
        totalFiles = 0
        
        if fhour > afterHour:
            levDict = after_levs
        else:
            levDict = levs
    
        for var in vars:
            # if searching for 0-h precip, will skip and go to the next variable    
            if var == 'APCP' and fhour == 0:
                continue
            for lev in levDict[var]:
                # force quit if too long after init time
                # prevents at infinite loop
                diff = startTime - cycleDT
                if diff.total_seconds()/3600.0 > timeout and giveup:
                    print 'Too late for this model run.  Giving up.'
                    sys.exit()
        
                # This should really be a defined dictionary.
                if lev == 0:
                    levtype = 'SFC'
                    if var == 'PRMSL':
                        levtype = 'MSL'
                    if var == 'CWAT':
                        levtype = 'EATM'
                elif (lev == 2 or lev == 10):
                    levtype = 'TGL'
                else:
                    levtype = 'ISBL'
          
                filename = 'CMC_glb_{}_{}_{}_latlon.15x.15_{}_P{:03d}.grib2'.format(var,
                                                                                    levtype,
                                                                                    lev,
                                                                                    cycleStr,
                                                                                    fhour)
                localfile = os.path.join(download_dir, filename)
                if not os.path.exists(localfile):
                    totalFiles = totalFiles + 1
                    fileList.append(filename)
    
        for f in fileList:
            print "Queue file: {}".format(f)
            localfile = os.path.join(download_dir, f)
            fullURL = '{}/{}'.format(fullWebDir, f)
            (tmp,tmp1,var,levtype,lev,latlon,dstring,hourstring) = f.split("_")
            hourstring = hourstring.replace('.grib2', '')
            fullgridFileName = 'CMC25_{}_{}_{}_{}_{}.grib2'.format(var, levtype,
                                                                   lev, dstring,
                                                                   hourstring)
            TASKS.append((fullURL, localfile, fullgridFileName))

        for file, outfile in pool.map(requestWorker, TASKS):
            if file is not None:
                print "Back from worker: {}".format(file)
                wgrib_tasks.append((file, outfile))
            
        fileDict = postProcessFiles(wgrib_tasks, cycleStr, fhour)
        
        print "Sending {} to LDM".format(fhour)
        sys.stdout.flush()
        if fileDict:
            for region in fileDict:
                pqCmd = [pqinsert, '-f', 'EXP', fileDict[region]]
                subprocess.call(pqCmd)
        
def get_rdps():
    # CMC directory that holds the model data
    
    if len(sys.argv) < 3:
        if startTime.hour >= 2 and startTime.hour < 8:
            cycle = 0
        elif startTime.hour >=8 and startTime.hour < 14:
            cycle = 6
        elif startTime.hour >= 14 and startTime.hour < 20:
            cycle = 12
        else:
            cycle = 18
    else:
        cycle = int(sys.argv[2])

    cycleDT = datetime(startTime.year, startTime.month, startTime.day, cycle)
    cycleStr = cycleDT.strftime('%Y%m%d%H')
    print "Running for cycle: {}".format(cycleStr)
    # If running from cron, enable giveup so this doesn't run forever
    if os.isatty(sys.stdin.fileno()):
        giveup = False
    else:
        giveup = True
    
    if cleanOldFiles:
        print 'Removing old CMC_glb files...'
        for dirpath, dirnames, filenames in os.walk(download_dir):
            for file in filenames:
                if "CMC_reg" not in file:
                    continue
                curpath = os.path.join(dirpath, file)
                file_modified = datetime.fromtimestamp(os.path.getmtime(curpath))
                diff = startTime - file_modified
                if diff.days >= 1:
                    #print curpath
                    os.remove(curpath)
         
        print 'Removing old regional clip files...'
        for dirpath, dirnames, filenames in os.walk(output_dir):
            for file in filenames:
                if "CMC10" not in file:
                    continue
                curpath = os.path.join(dirpath, file)
                file_modified = datetime.fromtimestamp(os.path.getmtime(curpath))
                diff = startTime - file_modified
                if diff.days >= 1:
                    #print curpath
                    os.remove(curpath)
    
    levs = {}
    for var in vars:
        if var in ['DEPR','HGT']:
            levs[var] = all_plevs
        if var in ['TMP']:
            levs[var] = all_plevs + [2]
        if var in ['UGRD','VGRD']:
            levs[var] = all_plevs + [10]
        if var in ['ABSV']:
            levs[var] = [500]
        if var in ['PRMSL','APCP','TCDC','CAPE','4LFTX','CWAT','PRES']:
            levs[var] = [0]
        if var in ['DPT']:
            levs[var] = [2]
    
    for fhour in fhours:
        fullWebDir = '{}/{:02d}/{:03d}'.format(URL_BASE, cycle, fhour)
    
        fileList = []
        TASKS = []
        wgrib_tasks = []
        missingfiles = 0
    
        totalFiles = 0
        
        for var in vars:
            # if searching for 0-h precip, will skip and go to the next variable    
            if var == 'APCP' and fhour == 0:
                continue
            for lev in levs[var]:
                # force quit if too long after init time
                # prevents at infinite loop
                diff = startTime - cycleDT
                if diff.total_seconds()/3600.0 > timeout and giveup:
                    print 'Too late for this model run.  Giving up.'
                    sys.exit()
        
                # This should really be a defined dictionary.
                if lev == 0:
                    levtype = 'SFC'
                    if var == 'PRMSL':
                        levtype = 'MSL'
                    if var == 'CWAT':
                        levtype = 'EATM'
                elif (lev == 2 or lev == 10):
                    levtype = 'TGL'
                else:
                    levtype = 'ISBL'
          
                filename = 'CMC_reg_{}_{}_{}_ps10km_{}_P{:03d}.grib2'.format(var,
                                                                             levtype,
                                                                             lev,
                                                                             cycleStr,
                                                                             fhour)
                localfile = os.path.join(download_dir, filename)
                if not os.path.exists(localfile):
                    totalFiles = totalFiles + 1
                    fileList.append(filename)
    
        for f in fileList:
            localfile = os.path.join(download_dir, f)
            fullURL = '{}/{}'.format(fullWebDir, f)
            (tmp,tmp1,var,levtype,lev,latlon,dstring,hourstring) = f.split("_")
            hourstring = hourstring.replace('.grib2', '')
            fullgridFileName = 'CMC10_{}_{}_{}_{}_{}.grib2'.format(var, levtype,
                                                                   lev, dstring,
                                                                   hourstring)
            TASKS.append((fullURL, localfile, fullgridFileName))

        for file, outfile in pool.map(requestWorker, TASKS):
            if file is not None:
                print "Back from worker: {}".format(file)
                wgrib_tasks.append((file, outfile))
            
        fileDict = postProcessFiles(wgrib_tasks, cycleStr, fhour)
        
        print "Sending {} to LDM".format(fhour)
        sys.stdout.flush()
        if fileDict:
            for region in fileDict:
                pqCmd = [pqinsert, '-f', 'EXP', fileDict[region]]
                subprocess.call(pqCmd)

def postProcessFiles(tasks, cycleStr, fhour):
    catfileDict = {}
    
    for region in regionsToRun:
        TASKS = []
        catfile = None
        clippedFiles = []
                        
        for task in tasks:
            infile, outfile = task
            p, f = os.path.split(infile)
            (tmp,tmp1,var,levtype,lev,latlon,dstring,hourstring) = f.split("_")
            domainLon = '{}:{}'.format(clipBounds[GRID_TYPE][region]['minX'],
                                       clipBounds[GRID_TYPE][region]['maxX'])
            domainLat = '{}:{}'.format(clipBounds[GRID_TYPE][region]['minY'],
                                       clipBounds[GRID_TYPE][region]['maxY'])
            if clipBounds[GRID_TYPE][region]['clipType'] == 'ij':
                clipOp = '-ijsmall_grib'
            else:
                clipOp = '-small_grib'
            # Check to see if we should clip to NH
            if GRID_TYPE == 'GDPS' and var in nhVars:
                if int(lev) in nhVars[var]:
                    # Subgrid to all of northern hemisphere
                    print "Setting clip bounds to NH..."
                    domain = nhbounds
                    (domainLon, domainLat) = domain.split()
            subgridFileName = '{}-{}'.format(region, outfile)
            outgrib = os.path.join(output_dir, subgridFileName)
            print "Clipping to {} domain: {} {}".format(region, domainLon, domainLat)
            wgrib2_cmd = [wgrib2, infile, '-set_grib_type', 'c3',
                          clipOp, domainLon, domainLat, outgrib]
            TASKS.append(wgrib2_cmd)
        for file in pool.map(wgribWorker, TASKS):
            clippedFiles.append(file)

        ######################################################################################################################
        #
        # This is a hack because for some reason the Canadian 25 km model has some constants in prmsl that are defined
        # as 4294967295 which is the largest 32 bit integer allowed. This appears to be an error and setting these
        # constants to 0 like the GFS has makes it play well with AWIPS. This should be removed if the CMC
        # corrects this issue. mark loeffelbein 12/22/14
        #
        # degrib had to be used with -I to find that the level was -10 instead of 0 for the MSL level. Once the large ints 
        # were removed the problem went away.
        #
        ####################################################################################################################### 

        for inpath in clippedFiles:
            p, infile = os.path.split(inpath)
            if "_PRMSL_" in infile or "_CWAT_" in infile:
                print "Fixing PRMSL or CWAT..."
                tmpfile = inpath+'.tmp'
                shutil.move(inpath, tmpfile)
                
                grbs = pygrib.open(tmpfile)
                with open(inpath,'wb') as grbout:
                    for grb in grbs:
                        obj = grb._all_keys
                        for attr in obj:
                            if isinstance(getattr(grb,attr),int):
                                if int(getattr(grb,attr)) == 4294967295:
                                    grb[attr]=0
        
                        msg=grb.tostring()
                        grbout.write(msg)
                grbs.close()
                grbout.close()
                os.remove(tmpfile)
            if GRID_TYPE == 'GDPS':
                catFileName = '{}-CMC25_{}_P{:03d}.grib2'.format(region, cycleStr, fhour)
            else:
                catFileName = '{}-CMC10_{}_P{:03d}.grib2'.format(region, cycleStr, fhour)
            catfile = os.path.join(output_dir, catFileName)
            with open(catfile, 'ab') as cf:
                shutil.copyfileobj(open(inpath, 'rb'), cf)
        if catfile is not None:
         catfileDict[region] = catfile
            
    return catfileDict
        

if __name__ == '__main__':
    print "GRID_TYPE = {}".format(GRID_TYPE)
    fhours = []
    if GRID_TYPE == 'GDPS':
        #URL_BASE = 'https://dd.weather.gc.ca/model_gem_global/25km/grib2/lat_lon'
        URL_BASE = 'https://dd.meteo.gc.ca/model_gem_global/15km/grib2/lat_lon'
        for fhour in range(fhourmin, GDPS_fhourmax+GDPS_fhourstep, GDPS_fhourstep):
            # After GDPS_fhourmaxShort, only get 6-hr data
            if fhour > GDPS_fhourmaxShort and (fhour % 6) != 0:
                continue
            fhours.append(fhour)
        sess = get_session(URL_BASE)
        get_gdps()
    elif GRID_TYPE == 'RDPS':
        ##URL_BASE = 'https://dd.weather.gc.ca/model_gem_regional/10km/grib2'
        URL_BASE = 'https://dd.meteo.gc.ca/model_gem_regional/10km/grib2/'
        fhours = range(fhourmin, RDPS_fhourmax+RDPS_fhourstep, RDPS_fhourstep)
        sess = get_session(URL_BASE)
        get_rdps()
    else:
        print "Unknown GRID TYPE {}".format(GRID_TYPE)
        sys.exit(1)
