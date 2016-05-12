#!/bin/env python
#   Usage: On a jobsub worker node, just before returning the output files:
#   python WorkerNodeMetadataUpdate.py 
#
#   program to:
#      1) Check to see if there are any completed digits files and if there are:
#      2) For each, check if there is a .json file for them and if there is:
#      3) Update it with more metadata fiels
#
#  Assumes the .json files it finds are the output of root_metadata.py as it performed 2016.05.06 in lariatsoft v05_09_00
#
import glob
import sys
import os
import subprocess
import time
import shutil
import xml.etree.ElementTree as ET
import json
import samweb_client

#
#   Count the number of returned files in the local directory here on the worker node
#
datafilelist = glob.glob('./*.root')
if len(datafilelist) == 0: exit ("No data files found.")

#  Connect to SAM database
#
samweb=samweb_client.SAMWebClient(experiment="lariat")

#
#   Loop through data files which are present
#
nfilesprocessed = 0
for datafile in datafilelist:
    print datafile
    jsonfilename = datafile+".json"
    print jsonfilename
    # Skip if no JSON file was created
    if not os.path.isfile(jsonfilename): continue
    # Open the file we got and load it into a dictionary
    fp = open(jsonfilename,"r")
    thejson=json.load(fp)
    fp.close()

    # Set some string values by hand
    thejson["data_tier"] = "digits"
    thejson["file_format"] = "artroot"
    thejson["file_size"] = int(thejson["file_size"])

    runnumber = thejson["subruns"][0][0]
    subrunnumber = thejson["subruns"][0][1]
    eventcount = thejson["events"]
    parentfilename = 'lariat_r{:06}_sr{:04}.root'.format(runnumber,subrunnumber)
    print "Parent file for metadata forwarding: ",parentfilename

    # Delete a few elements which are invalid SAM metadata
    deletethese = ('events','subruns')
    for k in deletethese: del thejson[k]
    
    # Get some more valus from sam_metadatadumper. Lots of gymnastics! 
    # Get a string...
    samdumpstr = subprocess.check_output('sam_metadata_dumper '+datafile, shell=True)
    # ...to write a file...
    dumpfp = open('tempjunk','w+')
    dumpfp.write(samdumpstr)
    # ...which we close and then open...
    dumpfp.close()
    dumpfp = open('tempjunk','r')
    # ...so we can dump it into a dictionary!
    dumpjson = json.load(dumpfp)
    # But we don't need the first level of the hierarchy. 
    # Tunnel down and make what we find the whole dictionary.
    mainkey = dumpjson.keys()[0]
    dumpjson = dumpjson[mainkey]

    # # Key list: #
    # "file_type",    
    # "process_name",
    # "data_tier",
    # "data_stream",
    # "file_format",
    # "file_format_era",
    # "file_format_version",
    # "start_time",
    # "end_time",
    # "event_count", <-- Correct name.
    # "first_event",
    # "last_event",
    # "parents"
    dumpthese = ("data_stream", "file_format",
                 "start_time", "end_time", 
                 "event_count", "first_event", "last_event", "parents")
    for key in dumpthese:
        thejson[key] = dumpjson[key]

    # For some reason first_event is being assigned bad values.  Fix them:
    # [0]: run, [1]: subrun, [2]: event number
    # Want ints, not these tuples we get!
    thejson["last_event"] = thejson["last_event"][2]
    thejson["first_event"] = 1 + thejson["last_event"] - thejson["event_count"] # Event number incorrectly set in first. Take from last. 

    # Get the metadata of the parent raw file
    parentjson = samweb.getMetadata(parentfilename)

    # ...and add it to the outgoing json object
    intlist = ("secondary.intensity", "secondary.momentum", "tertiary.magnet_current", "tertiary.number_MuRS", "detector.shield_voltage", "detector.cathode_voltage", "detector.induction_voltage", "detector.collection_voltage")

    stringlist = ("file_type", "tertiary.punch_through", "tertiary.DSTOF", "tertiary.halo_paddle", "tertiary.MWPC3", "tertiary.MWPC2", "tertiary.MWPC1", "tertiary.MWPC4", "detector.pmt_ham", "detector.sipm_sensl", "tertiary.magnet_polarity", "tertiary.cosmic_counters", "tertiary.muon_range_stack", "secondary.polarity", "detector.sipm_ham", "tertiary.cherenkov2", "tertiary.beam_counters", "tertiary.cherenkov1", "tertiary.USTOF", "detector.pmt_etl")

    for key, value in parentjson.iteritems():
        if key in stringlist:
            thejson[key] = parentjson[key]
        elif key in intlist:
            thejson[key] = int(parentjson[key])

    thejson['application'] = {}
    thejson['application']['name']   = "lariatsoft"
    thejson['application']['family'] = "art"
    
    # Finally, grab environment variables and add them to the metadata:
    environ_vars = {'LARIATSOFT_VERSION':'version',}
    for k,v in environ_vars.iteritems():
        if k in os.environ.keys():
            if v == 'version':
                thejson['application'][v] = os.environ[k]
            else: thejson[v] = os.environ[k]
    # close for k,v in environ_vars.iteritems()

    newfile = open(jsonfilename,'w+')
    newfile.write(json.dumps(thejson, indent=3, sort_keys=True))
    newfile.write('\n')
    newfile.close()

    nfilesprocessed+=1
