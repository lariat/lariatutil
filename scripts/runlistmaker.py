import sys
import subprocess
import argparse
import re
import os
import samweb_cli
import xml.etree.ElementTree as ET

parser = argparse.ArgumentParser(description="Get files from SAM.")
parser.add_argument('firstrun', type=int, help="first run number in series")
parser.add_argument('lastrun', type=int, help="last run number in series")
parser.add_argument('-o', '--outfilename',type=str, help="Config filename to make or overwrite", default="config.xml")
args = parser.parse_args()

firstrun = args.firstrun
lastrun = args.lastrun
outfilename = args.outfilename
debug = False

print 'Output filename :{}'.format(outfilename)

if firstrun > lastrun:
    tmp = firstrun
    firstrun = lastrun
    lastrun = tmp
    print 'Swapped firstrun and lastrun for you: {0:} < {1:}.'.format(firstrun, lastrun)

output_dir = './'
runlist = []
run_NONE = []
run_e0 = []
run_e1 = []
run_e2 = []
run_e3 = []
run_e4 = []
countlist = []

# configure samweb
samweb = samweb_cli.SAMWebClient(experiment='lariat')

for runnumber in xrange (firstrun, lastrun+1):
    count = 0
    query = "run_number {} and file_format artroot and data_tier raw".format(runnumber)
    count = samweb.countFiles(query)
    if count > 0:
        if debug: print count
        runlist.append(runnumber)
        if count < 10: run_e0.append(runnumber)
        elif count < 100: run_e1.append(runnumber)
        elif count < 1000: run_e2.append(runnumber)
        elif count < 10000: run_e3.append(runnumber)
        else: run_e4.append(runnumber)
    else: run_NONE.append(runnumber)

print 'Runs with zero files:{0:}'.format( len(run_NONE) )
print 'Runs with O(10e0) files:{0:}'.format( len(run_e0) )
print 'Runs with O(10e1) files:{0:}'.format( len(run_e1) )
print 'Runs with O(10e2) files:{0:}'.format( len(run_e2) )
print 'Runs with O(10e3) files:{0:}'.format( len(run_e3) )
print 'Runs with O(10e4) files:{0:}'.format( len(run_e4) )

if len(runlist) < 1: 
    exit('No runs in range {0:}-{0:} have any input files.'.format(firstrun, lastrun))

# Should we be smarter about the order in which runs are scheduled to run? 
# For now, proceeding the easy, dumb way.

import xml.dom.minidom as minidom
def prettify(elem):
    """Return a pretty-printed XML string for the Element.
    """
    rough_string = ET.tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="   ")

def create_config_xml():
    
    outfile = open(outfilename,'w')
    config_el = ET.Element('config')
    # Need a workdir...
    workdir_el = ET.SubElement(config_el, 'workdir')
    workdir_el.text = '/lariat/app/users/lariatpro/lariatsoft/develop/production/job'
    # ...a project_xml...
    project_xml_el = ET.SubElement(config_el, 'project_xml')
    project_xml_el.text = '/lariat/app/users/lariatpro/lariatsoft_v01_09_00/srcs/lariatutil/project.xml'
    # ...and the run list itself
    runs_el = ET.SubElement(config_el, 'runs')
    runs_eltext = '\n'
    for run in runlist:
        runs_eltext = runs_eltext + '      '+str(run)+'\n'
    runs_el.text = runs_eltext+'   '
    # Make a new tree from the this Element and its SubElements
    tree = ET.ElementTree(config_el)
    root = tree.getroot()
    print prettify(root)
    outfile.write(prettify(root))


while (True):
    if os.path.isfile(outfilename):
        print outfilename+' exists. Runs listed:',
        tree = ET.parse(outfilename)
        e = tree.find("runs")
        print e.text
    
        reply = raw_input('Overwrite this file? (Y/n)')
        if reply == 'n': 
            altname = raw_input('Alternate filename:')
            outfilename = altname
            print 'Creating {0:}: \n'.format(outfilename)
            create_config_xml()
            break
        elif reply == '' or reply.lower() == 'y':
            print 'Overwriting {0:}: \n'.format(outfilename)
            create_config_xml()
            break
        else: 
            print 'Say what?'
            continue
    else:
        print 'Creating {0:}: \n'.format(outfilename)
        create_config_xml()
        break

    
