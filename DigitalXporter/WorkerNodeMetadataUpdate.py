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
    # Example: lariat_digit_r008124_sr0009_20160422T164208.root
    jsonfilename = datafile+".json"
    print jsonfilename

    # Open a new file with this name for writing into.
    f = open(jsonfilename,"w+")
    # Option to skip if no JSON file was created
    if not os.path.isfile(jsonfilename): continue

    # Make a small dictionary.
    metadata = {}
    metadata["data_tier"] = 'digits'
    metadata["file_format"] = 'artroot'

    # Dump the small dictionary into the file as JSON, then close the file.
    json.dump(metadata, f)
    f.close()

    # Re-open the dictionary and load it into a JSON object
    fp = open(jsonfilename,"r")
    thejson = json.load(fp)
    fp.close()

    # Get the metadata of the parent raw file
    parentfilename = datafile[:29]+".root"
    parentfilename = parentfilename.replace('_digit_', '_')
    parentfilename = os.path.basename(parentfilename)
    #print parentfilename
    parentjson = samweb.getMetadata(parentfilename)
    # ...and add the metadata to the outgoing json object
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
