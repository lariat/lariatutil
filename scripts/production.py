#!/usr/bin/env python

#/////////////////////////////////////////////////////////////
# Name:    TBD
# Date:    28 September 2015
# Author:  <YOUR NAME HERE!>
#/////////////////////////////////////////////////////////////
# FOR PRODUCTION USE ONLY.
#/////////////////////////////////////////////////////////////

import sys
import os.path
import shutil
import argparse
import subprocess
import xml.etree.ElementTree as ET
import psycopg2

import samweb_client

def fast_read(samweb, filename_list):
    """
    Fetch all the files from a list of files using a single
    SAM command for all the files. This will fail if the
    number of files is too large. How large is too large?

    """

    file_list = []

    filename_dict = samweb.locateFiles(filenameorids=filename_list)

    for filename in filename_list:
        system = filename_dict[filename][0]['system']
        full_path = \
            filename_dict[filename][0]['full_path'].split(system + ':')[1] \
            + '/' + filename
        file_list.append(full_path)

    return file_list

def slow_read(samweb, filename_list):
    """
    Fetch all the files from a list of files using one SAM
    command per file. This will not fail of the number of
    files is too large.

    """

    file_list = []

    for filename in filename_list:
        filename_dict = samweb.locateFile(filenameorid=filename)
        system = filename_dict[0]['system']
        full_path = \
            filename_dict[0]['full_path'].split(system + ':')[1] \
            + '/' + filename
        file_list.append(full_path)

    return file_list

# parser for command-line options
parser = argparse.ArgumentParser(description="Production jobs.")
parser.add_argument(
    '-x', '--xml', type=str, required=True, help="XML configuration file")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument(
    '--prepare', action='store_true',
    help="Prepare the input file lists and project.py XML files")
group.add_argument(
    '--submit', action='store_true', help="Submit jobs")
group.add_argument(
    '--status', action='store_true', help="Print status of jobs")
group.add_argument(
    '--check', action='store_true', help="Check output of jobs")
group.add_argument(
    '--makeup', action='store_true', help="Submit make-up jobs")
group.add_argument(
    '--clean', action='store_true', help="Clean output of jobs")

# parse parser arguments
args = parser.parse_args()
config_xml_file = args.xml
prepare = args.prepare
submit = args.submit
status = args.status
check = args.check
makeup = args.makeup
clean = args.clean

# parse configuration XML file
xml_tree = ET.parse(config_xml_file)
xml_root = xml_tree.getroot()
project_xml = xml_root.findall('project_xml')[0].text
runs = map(int, xml_root.findall('runs')[0].text.split())
work_dir = xml_root.findall('work_dir')[0].text

# please, please open a connection to the database!
please = open('/lariat/app/home/lariatpro/lariat_prd_passwd')
please = please.read()
conn = psycopg2.connect(database='lariat_prd',port='5443',host='ifdb02.fnal.gov',user='lariat_prd_user', password = please)
conn.autocommit = True  # When we execute queries, we mean it!

# Get the cursor, which sends all the db queries.
dbcur = conn.cursor()

# Python is just another dialect of Parseltongue.
#
#    --..,_                     _,.--.
#       `'.'.                .'`__ o  `;__.
#          '.'.            .'.'`  '---'`  `
#            '.`'--....--'`.'
#              `'--....--'`
#

# prepare for trouble
if prepare:

    # configure samweb
    samweb = samweb_client.SAMWebClient(experiment='lariat')

    # Register this campaign in the processsing database.
    lariatsoft_rel = 'Dummy'
    dbquery = 'INSERT INTO prodcampaigns ( launchtime, lariatsoft_tag) VALUES (now(), \'{}\');'.format(lariatsoft_rel)
    dbcur.execute(dbquery)
    if 'INSERT 0 1' not in dbcur.statusmessage: exit ("Unable to register this campaign in the database.")

    # Get the campaign number we were just assigned
    dbcur.execute('SELECT prodcampaignnum FROM prodcampaigns ORDER BY launchtime DESC LIMIT 1;')
    result = dbcur.fetchall() # Returns a list (length 1) of tuples (only want the 0th element)
    prodcampaignnum = result[0][0] #...only want the integer within. 
    
    # loop over runs
    for run in runs:

        # notify via stdout
        print "\nPreparing project.py files for run {}.\n".format(run)

        # add leading zeros to run number
        run_str = str(run).zfill(6)

        # working directory for this run
        directory = work_dir + '/r' + run_str

        # create directory if it does not exist
        if not os.path.exists(directory):
            os.makedirs(directory)

        # resgister this run in this campaign in the processing database
        dbquery = 'INSERT INTO runsofflineprocessed (runnumber, prodcampaignnum, work_dir, status) VALUES (%s,%s,%s,%s);'
        deets = (run, prodcampaignnum, directory, 'Not launched')
        dbcur.execute(dbquery, deets)

        # input list file
        input_list_path = directory + '/input_r' + run_str + '.list'

        # if input list file does not exist, create it
        if not os.path.isfile(input_list_path):

            # SAM query
            query = "run_number {} and file_format artroot".format(run)

            # fetch list of files from SAM
            filename_list = samweb.listFiles(dimensions=query)

            # sort list of files by their filename
            filename_list.sort()

            # get number of files
            number_files = len(filename_list)

            print "Run: {}; number of files: {}".format(run_str, number_files)

            # if there are no files, remove the working directory for this run
            if number_files < 1:
                print "Removing directory: {}".format(directory)
                # remove working directory for run
                shutil.rmtree(directory)
                # move on to the next run
                continue

            # attempt fast mode
            try:
                file_list = fast_read(samweb, filename_list)

            # fall back to slow mode if fast mode fails
            except:
                file_list = slow_read(samweb, filename_list)

            # write file list to file
            with open(input_list_path, 'w') as input_list_file:
                for file in file_list:
                    print >> input_list_file, file

        else:
            # get number of files
            number_files = sum(1 for line in open(input_list_path, 'r'))

            # if there are no files, remove the working directory for this run
            if number_files < 1:
                print "Removing directory: {}".format(directory)
                # remove working directory for run
                shutil.rmtree(directory)
                # move on to the next run
                continue

        # XML file for project.py
        project_xml_path = directory + '/project_r' + run_str + '.xml'

        # if XML file does not exist, create it
        if not os.path.isfile(project_xml_path):

            # read in project.py XML template
            project_xml_template_file = open(project_xml, 'r')
            project_xml_template = project_xml_template_file.read()
            project_xml_template_file.close()

            # configure run-dependent parameters
            xml_config = {
                'run_number': run_str,
                #'number_files': number_files, # this might not be necessary
                'input_list_file': input_list_path,
                }

            # write XML to file
            with open(project_xml_path, 'w') as project_xml_file:
                project_xml_file.write(project_xml_template % xml_config)

# utilize project.py
if submit or status or check or makeup or clean:

    # get project.py action
    if submit:
        action = '--submit'
    elif status:
        action = '--status'
    elif check:
        action = '--check'
    elif makeup:
        action = '--makeup'
    elif clean:
        action = '--clean'
    else:
        print "\nNo project.py action found. Exiting...\n"
        sys.exit(1)

    # loop over runs
    for run in runs:

        # add leading zeros to run number
        run_str = str(run).zfill(6)

        # working directory for this run
        directory = work_dir + '/r' + run_str

        # continue to next run if working directory does not exist
        if not os.path.exists(directory):
            print "\nWorking directory for run %s does not exist:\n\n" % run \
                  + "    " + directory + "\n\ncontinuing..."
            continue

        # exit if working directory does not exist
        #if not os.path.exists(directory):
        #    print "\nWorking directory for run %s does not exist:\n\n" % run \
        #          + "    " + directory + "\n\nExiting..."
        #    sys.exit(1)

        # input list file for this run
        input_list_path = directory + '/input_r' + run_str + '.list'

        # continue to next run if input list file does not exist
        if not os.path.isfile(input_list_path):
            print "\nInput list for run %s does not exist:\n\n" % run \
                  + "    " + input_list_path + "\n\nContinuing..."
            continue

        # exit if input list file does not exist
        #if not os.path.isfile(input_list_path):
        #    print "\nInput list for run %s does not exist:\n\n" % run \
        #          + "    " + input_list_path + "\n\nExiting..."
        #    sys.exit(1)

        # XML file for project.py
        project_xml_path = directory + '/project_r' + run_str + '.xml'

        # continue to next run if XML file does not exist
        if not os.path.isfile(project_xml_path):
            print "\nXML file for run %s does not exist:\n\n" % run \
                  + "    " + project_xml_path + "\n\nContinuing..."

        # exit if XML file does not exist
        #if not os.path.isfile(project_xml_path):
        #    print "\nXML file for run %s does not exist:\n\n" % run \
        #          + "    " + project_xml_path + "\n\nExiting..."
        #    sys.exit(1)

        # command for running project.py
        cmd = [
            'project.py',
            '--xml',
            project_xml_path,
            '--stage',
            'digit',
            action,
            ]

        # notify via stdout
        print "\nExecuting command:\n"
        print "    " + " ".join(cmd) + "\n"

        # run command
        subprocess.check_call(cmd)
        # Need to capture and parse the output, and update the database accordingly: -d lariat_prd -p 5443 -h ifdb02.fnal.gov
