#
#   Usage: On a jobsub worker node, just before returning the output files:
#   python WorkerNodeMetadataUpdate.py 
#
#   program to:
#      1) Check to see if there are any completed digits files and if there are:
#      2) For each, check if there is a .json file for them and if there is:
#      3) Update it with more metadata fiels
#
import glob
import sys
import os
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
    fp = open(jsonfilename,"r")
    thejson=json.load(fp)
    fp.close()
    
    # Make a change or two
    thejson["data_tier"] = "digits"
    thejson["file_format"] = "artroot"
    
    runnumber = thejson["subruns"][0][0]
    subrunnumber = thejson["subruns"][0][1]
    parentfilename = 'lariat_r{:06}_sr{:04}.root'.format(runnumber,subrunnumber)
    print parentfilename
    thejson["parent"] = parentfilename

    # Get the metadata of the parent raw file
    parentjson = samweb.getMetadata(parentfilename)

    # ...and add it to the outgoing json object
    intlist = ("secondary.intensity", "secondary.momentum", "tertiary.magnet_current", "tertiary.number_MuRS", "detector.shield_voltage", "detector.cathode_voltage", "detector.induction_voltage", "detector.collection_voltage")

    stringlist = ("file_type", "file_format", "tertiary.punch_through", "tertiary.DSTOF", "tertiary.halo_paddle", "tertiary.MWPC3", "tertiary.MWPC2", "tertiary.MWPC1", "tertiary.MWPC4", "detector.pmt_ham", "detector.sipm_sensl", "tertiary.magnet_polarity", "tertiary.cosmic_counters", "tertiary.muon_range_stack", "secondary.polarity", "detector.sipm_ham", "tertiary.cherenkov2", "tertiary.beam_counters", "tertiary.cherenkov1", "tertiary.USTOF", "detector.pmt_etl")

    for key, value in parentjson.iteritems():
        if key in stringlist:
            thejson[key] = parentjson[key]
        elif key in intlist:
            thejson[key] = int(parentjson[key])

    newfile = open(jsonfilename,'w+')
    newfile.write(json.dumps(thejson, indent=3, sort_keys=True))
    newfile.write('\n')
    newfile.close()

    nfilesprocessed+=1
