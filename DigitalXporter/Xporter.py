#
#   Usage: python Xporter.py <dropbox directory> <campaign number or start campaign number> <end campaign number (if necessary)>
#
#
#   program to:
#      1) Check to see if there are any completed digits files and if there are:
#      2) move them to the drop box area
#      3) create a .json file for them
#
import sys
import os
import time
import shutil
import psycopg2
import filelock
import xml.etree.ElementTree as ET
import json
import samweb_client
from addzeros import addzeros
#
#  check to see dropbox folder exists
#
if (len(sys.argv) == 3):
    startcampaign = int(sys.argv[2])
    endcampaign = startcampaign
elif (len(sys.argv) == 4):
    startcampaign = int(sys.argv[2])
    endcampaign = int(sys.argv[3])
else:
    print 'Command: python Xporter.py <dropbox directory> <campaign # or start campaign #> <end campaign #>'
    sys.exit(1)
dropboxdir=sys.argv[1]
if (not os.path.isdir(dropboxdir)):
    print "Dropbox directory: ",dropboxdir," not found - please restart program"
    exit(3)
if (dropboxdir[len(dropboxdir)-1] != "/"):dropboxdir=dropboxdir+"/"
#
#
# check to see if there is another copy running
#
#lock = filelock.FileLock("/afs/fnal.gov/files/room2/randy/DigitalXporterInProgress")
#try:
#    lock.acquire(timeout=5)
#except filelock.Timeout as err:
#    print "Failed to get lockfile - exiting"
#    exit(0)
#
# connect to the laraitprd database
#
try:
    conn=psycopg2.connect(database="lariat_prd",user="lariatdataxport",password="lariatdataxport_321",host="ifdbprod2",port="5443")
    cur=conn.cursor()
    updatecur=conn.cursor()
except:
    print "Cannot connect to lariat_prd, exiting"
    exit(2)
#
#   Count the number of files in the dropbox directory and the sub directories
#
dropboxdirlist = [[dropboxdir,0]]
dropboxlist = os.listdir(dropboxdir)
for fff in dropboxlist:
    ff1=os.path.join(dropboxdir,fff)
    if (os.path.isfile(ff1)): dropboxdirlist[0][1]+=1
    if (os.path.isdir(ff1)):
        dropboxdirlist.append([ff1,len(os.listdir(ff1))])
#for [fff,flen] in dropboxdirlist: print fff,flen
#
# see if there any completed files
#
#flag="outfiles_retrieved"
flag = "complete"
dbtable = "runsofflineprocessed"
cur.execute("SELECT work_dir,prodcampaignnum,runnumber FROM "+dbtable+" WHERE status = '"+flag+"' and prodcampaignnum >= "+str(startcampaign)+" and prodcampaignnum <= "+str(endcampaign)+" AND archive_status IS NULL;")
#
#  Temporary test file
#
#cur.execute("SELECT work_dir, prodcampaignnum, runnumber FROM "+dbtable+" WHERE runnumber = 5578 and prodcampaignnum = 83 AND archive_status IS NULL;")
#
#  Connect to SAM database
#
samweb=samweb_client.SAMWebClient(experiment="lariat")
#
#  Go through the work directories
#
nrunsfound=0
nfilesprocessed=0
for line in cur:
    archiveflag="Archived"
    nrunsfound+=1
    workdir=line[0]
    campaignnum = str(line[1])
    runno = str(line[2])
    xmlfile = workdir+"/project_r"+addzeros(runno,6)+".xml"
#  
#  parse the .xml and get the relevant information 
#
    if (not os.path.isfile(xmlfile)):
        print "Error",xmlfile, "not found"
        archiveflag = ".xml not found"
        continue
    tree=ET.parse(xmlfile)
    root=tree.getroot()
#    for child in root:
#        print child.tag, child.text
#        if (len(child) != 0):
#            for grands in child:
#                print "     ",grands.tag,grands.text
    outputdir=root.findall("./stage/outdir")[0].text
    fclfile = root.findall("./stage/fcl")[0].text
#    print outputdir, fclfile
#
#  find the output directory (one directory down from outputdir)
#    Note: this code assumes one and only one directory in outputdir
#
    filelist = os.listdir(outputdir)
    outputfiledir = "None"
    for ff in filelist:
        fff = os.path.join(outputdir,ff)
        if (os.path.isdir(fff)):
            outputfiledir=fff
            break
    print outputfiledir
    if (outputfiledir=="None"):
        print "Error - Datafile subdirectory not found in",outputdir
        archiveflag = "Datafile subdirectory not found"
        updatecur.execute("UPDATE "+dbtable+" SET archive_status = '"+archiveflag+"' WHERE runnumber = "+runno+" AND prodcampaignnum = "+campaignnum+";")
        conn.commit()
        continue
#
#   Check output file names against expectations
#
    fdinputlist = open(outputfiledir+"/input.list","r")
    inputlist = fdinputlist.read().split("\n")
    OKfiles=[]
    for fff in inputlist[:len(inputlist)-1]:
#            print fff
        okrun = fff[6:17] #_r??????_sr
        oksubrun = fff[17:fff.rfind(".")]
        newname = "lariat_digit"+okrun+addzeros(oksubrun,4)+".root"
#        print fff,newname
        OKfiles.append(newname)
        OKfiles.append(newname+".json")
#    print OKfiles
    filelist = os.listdir(outputfiledir)
    outputfilelist = []
    for fff in filelist:
        fff1 = fff[:27]+fff[43:] #remove time from file
#        print fff1
        if (fff1 in OKfiles):
            OKfiles.remove(fff1)
            if (fff[len(fff)- 5:]==".root" and os.path.isfile(os.path.join(outputfiledir,fff+".json"))): 
                outputfilelist.append(fff)
#    print OKfiles
    if (len(OKfiles) != 0):
        print "Missing subrun files for",runno,":"
        archiveflag = "Missing subruns"
        for fff1 in OKfiles:
            print '\t',fff1
#    print outputfilelist
#
#  find directory to save the files in
#
    nfiles = len(outputfilelist)
    newdropbox = "None"
    for [dropboxdirname,dropboxdirlen] in dropboxdirlist:
        if (dropboxdirlen + 2* nfiles > 5000): continue
        newdropbox = dropboxdirname
        break
    if (newdropbox == "None"):
        print "All dropboxes full - Exiting"
        exit(0)
#
#   Process output files
#
    for ff in outputfilelist:
        print "From run",runno,"and campaign",campaignnum,"processing",ff
        nfilesprocessed+=1
        fp = open(os.path.join(outputfiledir,ff)+".json","r")
        outputjson=json.loads(fp.read())
        xrun = int(ff[14:20])
        if (xrun <= 3900):
            sbrun =ff[25:27]
        else:
            sbrun=ff[23:27]
        samfile ="lariat_"+ff[13:23]+sbrun+".root"
#        print samfile
        samjson=samweb.getMetadata(samfile)
#
# Update parameters
#
        samjson["event_count"]=int(outputjson["events"])
        samjson["parents"]=[samjson["file_name"]]
        brandnewname=ff
#        print brandnewname
        samjson["file_name"]=brandnewname
        samjson["file_size"]=int(outputjson["file_size"])
        samjson["data_tier"]="digits"
        samjson["fcl.name"]=fclfile
        samjson["checksum"] = '["enstore:'+str(outputjson["crc"]["crc_value"])+'"]'
        if "first_event" in outputjson:
            samjson["first_event"]=outputjson["first_event"]
        else:
            del samjson["first_event"]
        if "last_event" in outputjson:
            samjson["last_event"]=outputjson["last_event"]
        else:
            del samjson["last_event"]
        samjson["analysis.campaign"]=campaignnum
#
# Remove parameters
#
        del samjson["checksum"]
        del samjson["file_id"]
        del samjson["create_date"]
        if "update_date" in samjson: del samjson["update_date"]
        del samjson["user"]
        if "update_user" in samjson: del samjson["update_user"]
#        print samjson
#
#   Write out .json file and move data file to dropbox
#
        mvcmd = 'mv %s %s'%(os.path.join(outputfiledir,ff),os.path.join(newdropbox,brandnewname))
#        print mvcmd
        os.system(mvcmd)
        fdout=open(os.path.join(newdropbox,brandnewname+".json"),"w")
        json.dump(samjson,fdout)
        for index in range(len(dropboxdirlist)):
            if (newdropbox == dropboxdirlist[index][0]):
                dropboxdirlist[index][1]+=2
                print dropboxdirlist[index]
                break
#
#  Update database
#
    updatecur.execute("UPDATE "+dbtable+" SET archive_status = '"+archiveflag+"' WHERE runnumber = "+runno+" AND prodcampaignnum = "+campaignnum+";")
    conn.commit()
print "Number of runs found:",nrunsfound
print "Number of files processed",nfilesprocessed
