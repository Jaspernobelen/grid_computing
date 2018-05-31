#!/usr/bin/python2.7

"""

                          Documentation of Copy_all.py

  This is a script that checks whether files are stored on the GRID. Anything 'new' on the local directories will be copied. 
  This script also checks individual files; if there are any missing files, it will correctly transfer all the files to the GRID

  WRITTEN BY: J.C.P.Y. Nobelen - v1. 02-08-2018
                                 v2. 03-23-2018
  CONTACT: jaspern@nikhef.nl



                            How to call the script: Copy_all -i <int> -p <int>


             ARGUMENTS
             -i: Do I want to copy raw files?                      Answer in bits. 0 = N, 1 = Y
             -p: Do I want to copy processed files?                Answer in bits. 0 = N, 1 = Y

             The raw and processed files can be copied in the same script. You do not have to run it twice!


             CUSTOMISATION
             You can change the input folders of the script to whatever you'd like. These are the only variables to change in the script.
            
"""

import pprint
import subprocess,json
from subprocess import check_output
import re
import os,glob,sys
import argparse
import logging, datetime, time

#Testcase maken

# MAKE A TARBALL
#            We start with exporting your ui.sara.nl GRID proxy. I have placed mine in /user/jaspern/Modulation/gridcomputing/tmp/x509up_u8962.
#os.system('export /project/datagrid/anaconda/bin/python')
#os.system('export X509_USER_PROXY=/user/jaspern/Modulation/gridcomputing/tmp/robotproxy')

#            Definition of all the directories
logfile_dir = "/user/jaspern/Modulation/gridcomputing/log_files/"
database_dir = "/user/jaspern/Modulation/gridcomputing/databases/"
#inputlocal_dir = "/data/modulation/Raw_Data/combined/"
inputlocal_dir = "/data/modulation/Raw_Data/testsara/"                                                                      # Where is my raw data stored LOCALLY?
 #outputlocal_dir = "/dcache/xenon/jaspern/Modulation/processed/"                                                                  # Where is my processed data stored LOCALLY?
outputlocal_dir = "/dcache/xenon/jaspern/Modulation/test/"
inputgrid_dir = "gsiftp://gridftp.grid.sara.nl:2811/pnfs/grid.sara.nl/data/projects.nl/modulations/Tape/tar/"              # Where should I copy RAW data to?
outputgrid_dir = "gsiftp://gridftp.grid.sara.nl:2811/pnfs/grid.sara.nl/data/projects.nl/modulations/TapeAndDisk/Data/tar/" # Where should I copy PROCESSED data to?
srmout = 'srm://srm.grid.sara.nl:8443/pnfs/grid.sara.nl/data/projects.nl/modulations/TapeAndDisk/Data/tar/'
srmin = "srm://srm.grid.sara.nl:8443/pnfs/grid.sara.nl/data/projects.nl/modulations/Tape/tar/"
#checksum_dir = "https://webdav.grid.surfsara.nl/pnfs/grid.sara.nl/data/projects.nl/modulations/TapeAndDisk/Data/test/"
which_files = {}
not_copied = []
#missing_file = []
in_folder = []
#srmls -l srm://srm.grid.sara.nl:8443/pnfs/grid.sara.nl/data/projects.nl/modulations/TapeAndDisk/Data/tar/mx_n_20170830_0700.tar | grep = 'Checksum value'

json_cal = database_dir +'calibration_database.json'
json_ana = database_dir + 'analysis_database.json'


#First check -- are there any jobs running?
username = "jaspern"
number_of_queued_jobs  = "qselect -u " + username + " -s Q | wc -l"
number_of_running_jobs = "qselect -u " + username + " -s R | wc -l"

date = str(datetime.date.today()) #If we run our script every day, we shall get a log file every day!

fname = 'testing_rightnow'

#This can remove old log files
logfile_path = logfile_dir +'log_'+fname+'_'+date

if os.path.exists(logfile_path):
    subprocess.call(['rm','-r',logfile_path])
    print('Log files already exits. removing old log files...')

logging.basicConfig(filename=fname,level=logging.DEBUG)
logging.debug("Current time: ")
logging.debug(datetime.datetime.now())
logging.debug("Starting the Synchronizer script. . .")

#            Start of the Copy_all script: Dependent on your input, the copier will listen to you.

#def main():
#    parser = argparse.ArgumentParser(description='Welcome to the Copier! I will copy raw and processed data files to dCache.')
#    parser.add_argument('-i', '--raw',type = int, help="Copying all the raw data")
#    parser.add_argument('-p', '--process', type = int,  help="Copying all the processed data")
#
 #   args = parser.parse_args()
  #  rawOn = args.raw                      # Did the user order me to copy raw files?                                                                                                                                                                                         # 
 #   processedOn = args.process            # Did the user order me to copy processed files?  
 #   if (rawOn == 0):
 #       print(' ')
 #   else:
 #       localfolder = inputlocal_dir
 #       gridfolder = inputgrid_dir
 #       gridfolder_all = inputgrid_dir + '*'
 #       tarchecker(gridfolder_all)
 #       tarmaker(localfolder,gridfolder_all)
 #       syncer(localfolder,gridfolder)
 #       checksum(localfolder,gridfolder,srmin)
 #   if (processedOn == 0):
 #       print(' ')
 #   else:
 #      logging.debug('Let me check all the PROCESSED files & folders . . .') 
  #     logging.debug('-'*60)
   #    localfolderP = outputlocal_dir
   #    gridfolderP = outputgrid_dir
   #    gridfolder_allP = gridfolderP + '*'
   #    handlesP = ".root"
   #    run_all_the_functions(localfolderP,gridfolderP,gridfolder_allP)
   #    tarchecker(gridfolder_allP)
   #    tarmaker(localfolderP,gridfolder_allP)
   #    syncer(localfolderP,gridfolderP)
   #    checksum(localfolderP,gridfolderP,srmout)#
#
#       logging.debug('-'*60)
#    subprocess.call(['mv',fname,logfile_path]) 


def main():
    """        SPECIFYING THE FOLDERS                                                                                                                               Before we run the script, the script will check for any jobs on stoomboot. Instead of copying something many times, we will only start the script if
    all the processing has been finished.

                                               
    Because we don't want to put everything in the functions manually, I created two if-statements.                                                                                                       
    These if-statements will define our local & grid paths (they differ for raw and processed data).                                                                                                           Then it will just put everything in the do_it_all and voila: we have ourselves a running script! 
    """
    #subprocess.Popen(['ls', '-l'], stdout=subprocess.PIPE).communicate()[0] 
    is_done_r = int(subprocess.check_output(number_of_running_jobs,shell=True))
    is_done_q = int(subprocess.check_output(number_of_queued_jobs,shell=True))
    if (is_done_q == 0) and (is_done_r == 0):
        logging.debug("There are no jobs queued or running. Transfering files starting now!") #manual - algemeen stukje
    else: 
#        sleep(3600)
        if (is_done_q == 0) and (is_done_r == 0):
            logging.debug("There are no jobs queued or running. Transfering files starting now!")
#        else:
#            continue
#            sys.exit("There are jobs running at the moment. Please return when there are no jobs running!")

    parser = argparse.ArgumentParser(description='Welcome to the Copier! I will copy raw and processed data files to dCache.')
    parser.add_argument('-i', '--raw',type = int, help="Copying all the raw data")
    parser.add_argument('-p', '--process', type = int,  help="Copying all the processed data")

    args = parser.parse_args()
    rawOn = args.raw                      # Did the user order me to copy raw files?
    processedOn = args.process            # Did the user order me to copy processed files?
    logging.debug(parser.description)
    logging.debug(' ')
    #fname = 'Copied.log'
#coding convention (python standard library - pep8) flake8 = syntax checking -- 2 to 3 python script | git repository
    if (rawOn == 0):
        logging.debug('You did not choose to copy the raw files. Continuing to processed files!')
    else:
        logging.debug('Let me check all the RAW files & folders . . .')
        logging.debug('-'*60)
        localfolder = inputlocal_dir
        gridfolder = inputgrid_dir
        gridfolder_all = inputgrid_dir + '*'
        json_raw = database_dir+'rawdata_database.json'
        handles = ".bin"
        do_it_all2(handles,localfolder,gridfolder,gridfolder_all,srmin,json_raw) #niet zo'n goede functienaam
        logging.debug(' ')
        logging.debug(' ')
    if (processedOn == 0):
        logging.debug('You did not choose to copy the processed files. Exiting the script!')
        logging.debug('Finishing the Copying script. Bye!')
    else:
        logging.debug('Let me check all the PROCESSED files & folders . . .')
        logging.debug('-'*60)
        localfolderP = outputlocal_dir
        gridfolderP = outputgrid_dir
        gridfolder_allP = gridfolderP + '*'
        handlesP = ".root"
        json_proc = database_dir+'procdata_database.json'
        do_it_all2(handlesP,localfolderP,gridfolderP,gridfolder_allP,srmout,json_proc)
        logging.debug('-'*60)
        logging.debug('Checking the ANA & CAL files right now:')
        ana_cal_patcher(gridfolderP,localfolderP)
        ana_cal_checker(gridfolderP)
        ana_cal_syncer(gridfolderP,localfolderP)
        logging.debug('Checksums of ANA & CAL files:')
        ana_cal_checksum(localfolderP,gridfolderP,srmout)
    subprocess.call(['mv',fname,logfile_path])
    logging.debug("Current time: ")
    logging.debug(datetime.datetime.now())
    logging.debug("Quitting the Synchronizer. Bye!")

def read_json(json_fname):
    if os.path.exists(json_fname):
        global already_checked
        already_checked = []
        global bad_files
        bad_files = []
        global old_data
        old_data = []
        global good_data
        good_data = []
        global is_any_bad
        with open(json_fname) as f:
            old_data = json.load(f)
#            print(len(old_data))
#    print(old_data[0])
            for i in range(len(old_data)):
                if old_data[i]["Discrepancy"] == 0:
                    already_checked.append(old_data[i]["Name"])
                    good_data.append(old_data[i])
                if old_data[i]["Discrepancy"] == 1:
                    bad_files.append(old_data[i]["Name"])
#        return old_data,already_checked
    else: 
        already_checked = 0
        old_data = 0
        good_data = 0
        bad_files = 0
def read_ana_cal_json(json_cal,json_ana):
    global checked_cal
    checked_cal = []
    global bad_cal
    bad_cal = []
    global checked_ana
    checked_ana = []
    global bad_ana
    bad_ana = []
    if os.path.exists(json_cal):
        with open(json_cal) as f:
            global caldata
            caldata = []
            global good_cal 
            good_cal = []
            caldata = json.load(f)
            for i in range(len(caldata)):
                if caldata[i]["Discrepancy"] == 0:
                    checked_cal.append(caldata[i]["Name"])
                    good_cal.append(caldata[i])
                    if caldata[i]["Discrepancy"] == 1:
                        bad_cal.append(caldata[i]["Name"])
    else: 
        bad_cal = 0
        checked_cal = 0
        caldata = 0
    if os.path.exists(json_ana):
        with open(json_ana) as f:
            global anadata
            anadata = []
            global good_ana 
            good_ana = []
            anadata = json.load(f)
            for i in range(len(anadata)):
                if anadata[i]["Discrepancy"] == 0:
                    checked_ana.append(anadata[i]["Name"])
                    good_ana.append(anadata[i])
                    if anadata[i]["Discrepancy"] == 1:
                        bad_ana.append(anadate[i]["Name"])
    else: bad_ana,checked_ana,caldata = 0
    return checked_cal,checked_ana,bad_cal,bad_ana 

def exclude_latest_folder(localfolder):
    """ When we have raw and processed folders, the last folder may not be fully filled yet - we can see it in the folders, but there is currently a run going on with this folder.

    This is why we exclude the latest folder, both raw and processed in the synchronization. This way, when the run is finished (and processed), we have no problems synchronizing. 


    """
    all_individual_folders = glob.glob1(localfolder,'*')
    all_individual_folders = sorted(all_individual_folders,reverse=True) #Luckily, everything is saved in terms of dates, so we can easily find the latest folder (when sorted, the first element!)
    global most_recent
    most_recent = all_individual_folders[0]

    
def tarchecker(gridfolder_all):
    """ We store everything in .tar files, because that's more efficient. This function will check which .tar files are on the GRID.


    """
    print('Checking all the .tar files on the GRID')
    outputftp = subprocess.Popen(['uberftp','-ls',gridfolder_all],stdout = subprocess.PIPE,
                          stderr=subprocess.PIPE)
    o,  e = outputftp.communicate()                                                          # This creates our output. We will analyze the strings that come out of the uberftp -ls command.                                                                                
    lines = o.split('\n')                                                                    # Splitting every line                                                                                                                                                          
    file_name, folder_name = [], []
    global which_files
    which_files = []                                                                         # Which_Files is the list in which all tar files are stored. This will be used later (hence Global)                                                                             
    for line in lines:
        ls_fields = line.split()
        if len(ls_fields) <= 1:                                                              # Our output is like this: ['/pnfs/', 'projects.nl/', 'modulations/', 'TapeAndDisk/', '<folder>', 'mx_X_YYYYMMDD_HHMM.tar']                                                   
            continue                                                                         # Not all lines consist of information we need. We need at least the tar file, so anything <1 is wrong.                                                                    
        path_fields = ls_fields[-1].split('/')
        if path_fields[-1].startswith('mx_'):                                                # We restrict ourselves to 'mx_' folders.                                                                                      
           file_name = path_fields[-1]
           which_files.append(file_name)
    print('Finished on the GRID...')
    return which_files
def ana_cal_patcher(gridfolder,localfolder):
    """ ana_cal_patcher:: Patches the ANA_ & CAL_ files with an incorrect checksum.
    """
    if bad_cal != 0:
        for every in bad_cal:
            cal_file_local = localfolder + 'calibration/' + every
            cal_file_grid = gridfolder + 'calibration/' + every
            subprocess.call(['uberftp','-rm','-r',cal_file_grid])
            subprocess.call(['globus-url-copy','-r',cal_file_local,cal_file_grid])
            logging.debug('Fixed bad file ' + str(every) + '. Removed old file, made a new .tar file and re-synchronized!')
    if bad_ana != 0:
        for every in bad_ana:
            ana_file_local = localfolder + 'analysis/' + every
            ana_file_grid = localfolder + 'analysis/' + every
            subprocess.call(['uberftp','-rm','-r',ana_file_grid])
            subprocess.call(['globus-url-copy','-r',ana_file_local,ana_file_grid])
            logging.debug('Fixed bad file ' + str(every) + '. Removed old file, made a new .tar file and re-synchronized!')

def ana_cal_checker(gridfolder):
    calibrationfolder = 'calibration/*'
    analysisfolder = 'analysis/*'
    global ana_files_g
    ana_files_g = []
    global cal_files_g
    cal_files_g = []
    ana_output = gridfolder+analysisfolder
    cal_output = gridfolder+calibrationfolder
    ftpan = subprocess.Popen(['uberftp','-ls',ana_output],stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    ftpcal = subprocess.Popen(['uberftp','-ls',cal_output],stdout = subprocess.PIPE,stderr=subprocess.PIPE)

    o, e = ftpan.communicate()
    lines = o.split('\n')
    for line in lines:
        shown_fields = line.split()
        if len(shown_fields) <= 1:
            continue
        path_fields = shown_fields[-1].split('/')
        if path_fields[-1].startswith('ANA'):
            ana_name = path_fields[-1]
            ana_files_g.append(ana_name)
    p, f = ftpcal.communicate()
    callines = p.split('\n')
    for line in callines:
        shown_fields = line.split()
        if len(shown_fields) <= 1:
            continue
        path_fields = shown_fields[-1].split('/')
        if path_fields[-1].startswith('CAL'):
            cal_name = path_fields[-1]
            cal_files_g.append(cal_name)
    return cal_files_g, ana_files_g
#    print(cal_files_g,ana_files_g)

def ana_cal_syncer(gridfolder,localfolder):
    global ana_files_l
    global cal_files_l
    analysisfolder = localfolder + 'analysis/*'
    calfolder = localfolder + 'calibration/*'
    anagridfolder = gridfolder + 'analysis/'
    calgridfolder = gridfolder + 'calibration/'
    anadir  = glob.glob(analysisfolder)
    caldir = glob.glob(calfolder)
    ana_files_l = []
    cal_files_l = []
    for entry in anadir:
        rundir  = entry.split('/')[-1]
        ana_files_l.append(rundir)
    for entry in caldir:
        rundir = entry.split('/')[-1]
        cal_files_l.append(rundir)
    change_ana = list(set(ana_files_l)-set(ana_files_g))
    change_cal = list(set(cal_files_l)-set(cal_files_g))
    if len(change_ana) != 0:
        logging.debug('New ANA files appeared. Synchronizing...')
        for every in change_ana:
            locpath = 'file://'+analysisfolder + every
            gridpath = anagridfolder + every
            subprocess.call(['globus-url-copy','-r',locpath,gridpath])
            logging.debug('Synchronized all analysis files')
    if len(change_cal) != 0:
        logging.debug('New CAL files appeared. Synchronizing...')
        for every in change_cal:
            locpath = 'file://'+calfolder + every
            gridpath = calgridfolder + every
            subprocess.call(['globus-url-copy','-r',locpath,gridpath])
            logging.debug('Synchronized all calibration files')

def ana_cal_checksum(localfolder,gridfolder,srmfolder):
    new_cal_files,new_ana_files = ana_cal_checker(gridfolder) 
    caldatalog = []
    if checked_cal != 0:
        caldatalog = good_cal
    anadatalog = []
    if checked_ana != 0:
        anadatalog = good_ana
    for cal_file in new_cal_files:
        if checked_cal != 0 and cal_file in checked_cal[:]:
                print('I already checked this file: ' + str(cal_file) + ' It was okay!')
        else:
            loc_cal = localfolder + 'calibration/' + cal_file
            check_loc = subprocess.Popen(['adler32',loc_cal],stdout = subprocess.PIPE,stderr=subprocess.PIPE)
            cal,e  = check_loc.communicate()
            checksum_cal_loc = cal.split('\n')[0]
            srm_cal = srmfolder + 'calibration/' + cal_file
            checksum_grepping = ' | grep \"Checksum value\" | awk \'{print $4}\''
            cmd = 'srmls -l ' + srm_cal + checksum_grepping
            check_grid = subprocess.Popen(cmd,shell=True,stdout = subprocess.PIPE,stderr=subprocess.PIPE)
            calg,e = check_grid.communicate()
            checksum_cal_grid = calg.split('\n')[0]
            if checksum_cal_grid == checksum_cal_loc:
                logging.debug('CAL file: ' + str(cal_file) + ' has been transferred succesfully!')
                caldatalog.append({"Name" : cal_file, "Adler32 Checksum - LOCAL" : checksum_cal_loc, "Adler32 Checksum - GRID" : checksum_cal_grid, "Discrepancy" : 0 })
            else:
                logging.debug('File ' + str(cal_file) + ' doesn\'t have the same checksum. Watch out!!')
                caldatalog.append({"Name" : cal_file, "Adler32 Checksum - LOCAL" : checksum_cal_loc, "Adler32 Checksum - GRID" : checksum_cal_grid, "Discrepancy" : 1 })

    for ana_file in new_ana_files:
        if checked_ana != 0 and ana_file in checked_ana[:]:
                print('I already checked this file: ' + str(ana_file) + ' It was okay!' )
        else:
            loc_ana = localfolder + 'analysis/' + ana_file
            check_loc = subprocess.Popen(['adler32',loc_ana],stdout = subprocess.PIPE, stderr = subprocess.PIPE)
            ana,e = check_loc.communicate()
            checksum_ana_loc = ana.split('\n')[0]
            srm_ana = srmfolder + 'analysis/' + ana_file
            checksum_grepping = ' | grep \"Checksum value\" | awk \'{print $4}\''
            cmd = 'srmls -l ' + srm_ana + checksum_grepping
            check_grid = subprocess.Popen(cmd,shell=True,stdout = subprocess.PIPE,stderr=subprocess.PIPE)
            anag,e = check_grid.communicate()
            checksum_ana_grid = anag.split('\n')[0]
            if checksum_ana_grid == checksum_ana_loc:
                logging.debug('CAL file: ' + str(ana_file) + ' has been transferred succesfully!')
                anadatalog.append({"Name" : ana_file, "Adler32 Checksum - LOCAL" : checksum_ana_loc, "Adler32 Checksum - GRID" : checksum_ana_grid, "Discrepancy" : 0 })
            else:
                logging.debug('File ' + str(cal_file) + ' doesn\'t have the same checksum. Watch out!!')
                anadatalog.append({"Name" : ana_file, "Adler32 Checksum - LOCAL" : checksum_ana_loc, "Adler32 Checksum - GRID" : checksum_ana_grid, "Discrepancy" : 1 })
    
    if len(caldatalog) != 0:
        with open(json_cal,'w') as f:
            json.dump(caldatalog,f)
    if len(anadatalog) != 0:
        with open(json_ana,'w') as f:
            json.dump(anadatalog,f)



def localchecker(localfolder):
    """ Similar to tarchecker, this function simply checks the .tar files on the local storage.
    It also checks for the folders (because tarring has not happened yet for recent folders)

    """
    print('Checking local folders...')
    dirs = glob.glob(localfolder+'/mx_*')
    dirs = sorted(dirs, reverse=True)
    rundir = []
    for entry in dirs:
        run = entry.split('/')[-1]
        rundir.append(run)
    loc_foldernames = []
    tar_fnames = []
    for all_folders in rundir:
        if all_folders.endswith('.tar'): tar_fnames.append(all_folders)                       # So we make a list of every folder starting with 'mx_'. If they end with '.tar', we append 'tar_fnames', else we append 'loc_foldernames'. Both shall be used.
        else: loc_foldernames.append(all_folders)
    print('Finished locally...')
    return loc_foldernames,tar_fnames

def tarmaker(localfolder,gridfolder):
    """ TARKMAKER()::This function has two different
    If we find a discrepancy between local storage and GRID, this function checks:
        * Are there any folders missing?
        * Do we have a .tar file of the missing folder (i.e. does it exist and did we not copy this yet?)
    After creating the lists of missing tar files and folders, it will create new tar files of the untarred folders.    
    """
    print('Making the .tar files')
#    read_json(
    if bad_files !=  0:             #(her)gebruik je code/twee stukken opsplitsen
        #There must be bad files
        for bad in bad_files:
            badlocal = localfolder + bad 
            folder = localfolder + bad.split('.tar')[0]
            badgrid = gridfolder + bad
            subprocess.call(['rm','-r',badlocal])
            subprocess.call(['uberftp','-rm','-r',badgrid])
            #os.system('cd '+localfolder)
#            cmd = 'tar -C ' + folder + ' -cvf ' + badlocal + ' .' 
            subprocess.call(['tar','-C',folder,'-czvf',badlocal,'.'])
            subprocess.call(['globus-url-copy','-r',badlocal,badgrid])
            logging.debug('Fixed bad file ' + str(bad) + '. Removed old file, made a new .tar file and re-synchronized!')
    loc_foldernames,tar_fnames = localchecker(localfolder)
    foldername = []
    for all_tarfiles in which_files:
        foldername.append(all_tarfiles.split('.')[0])
    global mismatch_folders
    mismatch_folders = list(set(loc_foldernames)-set(foldername))
    global mismatch_tar
    mismatch_tar = list(set(tar_fnames)-set(which_files))
#    print(mismatch_tar)
    for every in mismatch_folders: #functie maken hiervoor
        if every in most_recent:
            logging.debug('I have stumbled upon the most recent folder: ' + str(every) + ' I won\'t make a tar file of this one!!')
        else:
            if os.path.exists(localfolder+every+'.tar'):
                logging.debug('Tar file already exists for folder ' + str(every) + '. Moving on...')
            else:
                tarfile = localfolder+every+'.tar'
                every_folder = localfolder+every+'/'
                logging.debug('Creating tar file ' + str(tarfile) + ' from folder ' + str(every) + '.')
                
                cmd = 'tar -C ' + every_folder + ' -cvzf ' + tarfile + ' .'
#                print(cmd)
                os.system('cd '+ localfolder)
                subprocess.call(['tar','-C',every_folder,'-cvzf',tarfile,'.'])           #tarfile
#                subprocess.call([cmd]) 
    print('Finished making the tar files...')
    return mismatch_tar, mismatch_folders

def syncer(localfolder,gridfolder):
    """          
            SYNCER:: This function simply synchronizes the mismatched .tar folders and the newly created .tar folders!
    """
    print('Synchronizing...')
    if len(mismatch_tar) != 0:
        for every in mismatch_tar: #First copy missing tar files on the GRID
            #print(every)
            tarfile = every
            local_location = localfolder + tarfile
            gridfolder_location = gridfolder + tarfile
            subprocess.call(['globus-url-copy','-r',local_location,gridfolder_location])
    if len(mismatch_folders) != 0:
        for every in mismatch_folders:
            print('Synchronizing folder: ' + str(every))
            if every in most_recent:
                logging.debug('I have stumbled (again) upon the most recent folder: ' + str(every) + 'I won\'t synchronize this one!!')
            else:
                tarfile = localfolder+every+'.tar'
                gridfolder_location = gridfolder + every + '.tar'
                every_folder = localfolder+every+'/'
                logging.debug('Copying tar file ' + str(tarfile) + ' from folder ' + str(every) + '.')
                subprocess.call(['globus-url-copy','-r',tarfile,gridfolder_location]) 

        #if len(mismatch_tar) > 0:
        #    for tar_file in mismatch_tar:
        #        if tar_file.split('.')[0] == every:
        #            logging.debug('Tar file already synchronized for ' + str(tar_file) + '. Moving on...')
        #        else:                                      #If we found a new folder that had no .tar file yet, we created a new tar file. Time to copy this one.                                                                            #                               
        #            tarfile = every+'.tar'
        #            local_location = localfolder + tarfile
        #            gridfolder_location = gridfolder + tarfile
         #           subprocess.call(['globus-url-copy','-r',local_location,gridfolder_location])
#Since we just copied the tar files, if we have a folder mismatch AND a tar mismatch of the same folder, this is already resolved!
       # else:                                      #If we found a new folder that had no .tar file yet, we created a new tar file. Time to copy this one.           
       #     tarfile = every+'.tar'
        #    local_location = localfolder + tarfile
         #   gridfolder_location = gridfolder + tarfile
          #  subprocess.call(['globus-url-copy','-r',local_location,gridfolder_location])
        print('Done!')

def checksum(localfolder,gridfolder_all,srmfolder,json_fname):
    """ dCache automatically creates a checksum for the newly copied file. This checksum (adler32) has to be the same
        as the adler32 checksum of the locally stored file.

        CHECKSUM():: checks whether every file is correctly copied and if the checksum is OK.
    """
#    read_json(json_fname)
    data_good = []
    if old_data != 0:
        data_good = good_data
    new_which_files = tarchecker(gridfolder_all)
    loc_foldernames,tar_fnames = localchecker(localfolder)
    for every_tarfile in new_which_files:
        if already_checked != 0 and every_tarfile in already_checked[:]:
                print('I already checked this .tar file: ' + str(every_tarfile) + ' It was okay!')
        else:
            print('I have not yet checked: ' + str(every_tarfile))
            #            sys.exit()
            local_tar = localfolder + every_tarfile
            check_loc = subprocess.Popen(['adler32',local_tar],stdout = subprocess.PIPE,stderr=subprocess.PIPE)
            b,e = check_loc.communicate()
            checksum_local=b.split('\n')[0]
            logging.debug('-'*60)
            logging.debug('Checksum for LOCAL file ' + str(every_tarfile) + ' (adler32): ' + str(checksum_local))
            srmtar = srmfolder + every_tarfile
            checksum_grepping = ' | grep \"Checksum value\" | awk \'{print $4}\''
            cmd = 'srmls -l ' + srmtar + checksum_grepping
            o = subprocess.Popen(cmd,shell=True,stdout = subprocess.PIPE,stderr=subprocess.PIPE)           #regex
            a,e = o.communicate()
            #wrapper voor return code
            checksum_grid = a.split('\n')[0]
            logging.debug('Checksum for GRID file ' + str(every_tarfile) + ' (adler32): ' + str(checksum_grid))
            if checksum_local == checksum_grid:
                logging.debug('File ' + str(every_tarfile) + ' has been transferred succesfully!')
                data_good.append({"Name" : every_tarfile, "Adler32 Checksum - LOCAL" : checksum_local, "Adler32 Checksum - GRID" : checksum_grid, "Discrepancy" : 0 })
            else:
                logging.debug('File ' + str(every_tarfile) + ' doesn\'t have the same checksum. Watch out!!')
                data_good.append({"Name" : every_tarfile, "Adler32 Checksum - LOCAL" : checksum_local, "Adler32 Checksum - GRID" : checksum_grid, "Discrepancy" : 1 })
    if len(data_good) != 0:
        with open(json_fname,'w') as f:            #dictionary
            json.dump(data_good,f)

#    if len(data_bad) != 0:
#        with open(json_fname,'a') as f:

#            json.dump(data_bad,f)
    logging.debug('-'*60)

#srmls -l srm://srm.grid.sara.nl:8443/pnfs/grid.sara.nl/data/projects.nl/modulations/TapeAndDisk/Data/tar/mx_n_20170830_0700.tar | grep "Checksum value" | awk '{print $4}'

def do_it_all2(handles,localfolder,gridfolder,gridfolder_all,srm,json_fname):
    """ This function executes all the scripts above """
    read_json(json_fname)
    read_ana_cal_json(json_cal,json_ana)
    exclude_latest_folder(localfolder)
    tarchecker(gridfolder_all)
    tarmaker(localfolder,gridfolder)
    syncer(localfolder,gridfolder)
    checksum(localfolder,gridfolder_all,srm,json_fname)
















        
def grid_analyzer(gridfolder_all):
    """ We store processed files in the following way:                                                                                                                                               
                                                                                                                                                                                                 
                                      MAIN FOLDER PROCESSED FILES                                                                                                                                
                                          ________|_______                                                                                                                                       
                                          |               |                                                                                                                                      
     --> THIS PART IS CHECKED HERE      FOLDER A        FOLDER B                                                                                                                                  
                                          |                                                                                                                                                      
                                        ___________                                                                                                                                              
                                       |FILE A     |                                                                                                                                           
                                       | .         |                                                                                                                                             
                                       | .         |                                                                                                                                             
                                       |FILE Z     |                                                                                                                                             
                                       | +         |                                                                                                                                             
     --> UNTIL HERE -----------------------------------------------------------
                                       |FOLDER 'CALIBRATION'|                                                                                                                                    
                                          |                                                                                                                                                      
                                         ___________                                                                                                                                             
                                         |FILE A     |                                                                                                                                           
                                         | .         |                                                                                                                                           
                                         | .         |                                                                                                                                                                                    |FILE Z     |                                                                                                                                          
                                                        
    """
    logging.debug('~'*60)
    logging.debug('COPY_ALL::GRID_ANALYZER --> I WILL MAKE A LIST OF ALL THE FILES ON THE GRID')
    logging.debug('~'*60)
    outputftp = subprocess.Popen(['uberftp','-ls',gridfolder_all],stdout = subprocess.PIPE,
                          stderr=subprocess.PIPE)
    o,  e = outputftp.communicate()                                                          # This creates our output. We will analyze the strings that come out of the uberftp -ls command.
#    to_strings = o.decode('utf-8')
#    print(type(o))
    lines = o.split('\n')                                                                    # Splitting every line
    file_name, folder_name = [], []
    global which_files
    which_files = []                                                                         # Which_Files is the list in which all files are stored. This will be used later (hence Global)
    for line in lines:
        ls_fields = line.split()
        if len(ls_fields) <= 1:                                                              # Our output is like this: ['A', 'B', 'C', 'D', 'folder', 'file']
            continue                                                                         # Not all lines consist of information we need. We need at least the file + folder, so anything <1 is wrong.
        path_fields = ls_fields[-1].split('/')
        if path_fields[-2].startswith('mx_'):                                                # We restrict ourselves to 'mx_' folders; analysis and calibration are synchronized later.
            file_name = path_fields[-1]
            which_files.append(file_name)
    return which_files

def grid_analyzer_deeper(localfolder,gridfolder): #This is for the folder structure that our processor works with.
    """ We store processed files in the following way:
 
                                      MAIN FOLDER PROCESSED FILES
                                          ________|_______
                                          |               |
                                       FOLDER A        FOLDER B
                                          |               
                                        ___________
                                       |FILE A     |
                                       | .         |
                                       | .         |
                                       |FILE Z     |
                                       | +         |
    ^ THIS PART IS CHECKED EARLIER
    --> THIS PART IS CHECKED HERE      |FOLDER 'CALIBRATION'| 
                                          |
                                         ___________                                                                                                                                                                                      |FILE A     |                                                                                                                                                                                    | .         |                                                                                                                                                                                    | .         |                                                                                                                                                                                    |FILE Z     |                                                                                                                                                 
                                         """
    logging.debug('~'*60)
    logging.debug('COPY_ALL::GRID_ANALYZER_DEEPER --> I WILL MAKE A LIST OF ALL THE PROCESSED/CALIBRATIONFILES ON THE GRID')
    logging.debug('~'*60)
    list_of_folders = which_files.keys()
    global all_files_cal_grid_list,all_files_cal_local_list
    all_files_cal_grid_list,all_files_cal_local_list = {},{}

    dirs = glob.glob(localfolder+'/mx_*') 
    dirs = sorted(dirs, reverse=True)
    rundir = []
    for entry in dirs:
        run = entry.split('/')[-1]
        rundir.append(run)
    for any_folder in rundir:
        files = glob.glob(localfolder+any_folder+'/calibration/'+'mx_*')
        files = sorted(files,reverse=True)

        for entry in files:
            single_file = entry.split('/')[-1]
            all_files_cal_local_list.setdefault(any_folder,[])
            all_files_cal_local_list[any_folder].append(single_file)
    for a_folder in list_of_folders:
        new_statement = gridfolder + a_folder + '/calibration/'

        outputftp = subprocess.Popen(['uberftp','-ls',new_statement],stdout = subprocess.PIPE,stderr=subprocess.PIPE)
        o,  e = outputftp.communicate() 
        lines=o.split('\n')
        file_name, folder_name = [], []
        for line in lines:
            ls_fields = line.split()
            if len(ls_fields) <= 1:
                continue
            path_fields = ls_fields[-1].split('/')
            all_files_cal_grid_list.setdefault(a_folder,[])
            all_files_cal_grid_list[a_folder].append(path_fields[-1])

def calibrationfolder_sync(localfolder,gridfolder):
# What I ideally need to do is when it has been checked, do not check it again? Flags??
    """
          If there's any mismatch in the calibration folders within the mx_ folders, this function will fix it!

    """
    local_folders = all_files_cal_local_list.keys()
    grid_folders = all_files_cal_grid_list.keys()

    missing_folder =list(set(local_folders)-set(grid_folders))
    for a_folder in missing_folder:
        if most_recent in a_folder:
            logging.debug('I found a missing folder, '+ str(most_recent) +' but it is the most recent one. Skipping . . . ')
        else:
            local_location = localfolder + a_folder + '/calibration/'
            grid_location = gridfolder + a_folder + '/calibration/'
            logging.debug('I found a missing folder; I will restore this right now! ')
            subprocess.call(['uberftp','-mkdir',grid_location])
            subprocess.call(['globus-url-copy','-r',local_location,grid_location])
            logging.debug('-'*60)
    for all_folders in grid_folders:
        missing_files = list(set(all_files_cal_local_list[all_folders])-set(all_files_cal_grid_list[all_folders]))
        grid_location = gridfolder + all_folders + '/calibration/'
        local_location = localfolder + all_folders + '/calibration/'

        if len(missing_files) != 0:
            logging.debug('There is a missing file in folder: ' + str(all_folders))
            subprocess.call(['uberftp','-rm','-r',grid_location])
            subprocess.call(['uberftp','-mkdir',grid_location])
            logging.debug('Created new blank folder: ' + str(grid_location))
            logging.debug('Starting file transfer. . .')
            subprocess.call(['globus-url-copy','-r',local_location,grid_location])
            logging.debug('Resynchronized folders!')
            logging.debug('-'*60)

def local_analyzer(localfolder):
    list_of_folders = which_files.keys()
    dirs = glob.glob(localfolder+'/mx_*')                                                    # This command is an analyzer for stored files. It's will get every file starting with mx_!
    dirs = sorted(dirs, reverse=True)
    rundir = []
    for entry in dirs:
        run = entry.split('/')[-1]
        rundir.append(run)
    global not_copied 
    not_copied = list(set(rundir)-set(list_of_folders))                                      # I created a list of the set(LOCAL)-set(GRID). This means, if there's new local files, it won't be == 0.
                                                                                             # If you define set(GRID)-set(LOCAL), you can find out if the local storage misses anything.

def file_analyzer(handles,localfolder,gridfolder):                                           # Handles will be '.root' or '.bin' dependent on raw/processed files.
    """            file_analyzer:                                                                                                                                                                                  
        WHAT DOES IT DO?                                                                                                                                                                                                           The funciton file_analyzer checks for every file on the grid. It then immediately checks for all the files that are stored locally.                                                                        If there's any mismatch, the file_analyzer will immediately patch the mistake by removing & replacing the folder! 
    """
    all_handles = "*"+handles
    list_of_folders = which_files.keys()                                                     # The list is divided as follows: 'folder' : file1,file2,file3, 'folder2' : file1 ...
    global in_folder
    in_folder = []
    for all_files in list_of_folders:
        grid_rootfiles = [x for x in which_files[all_files] if x.endswith(handles)]          # This is a list comprehension: in every folder, we check for files ending with .root or .bin!
        local_files = glob.glob1(localfolder+all_files+'/',all_handles)
        no_grid_rootfiles = len(grid_rootfiles)
        no_local_files = len(local_files)
        missing_file = list(set(local_files)-set(grid_rootfiles))
#add a flag/checksum

        if len(missing_file) != 0:
            in_folder.append(all_files)
            logging.debug("Missing the following file: " + str(missing_file) + " in folder " + str(all_files))

def exclude_latest_folder(localfolder):
    all_individual_folders = glob.glob1(localfolder,'*')
    all_individual_folders = sorted(all_individual_folders,reverse=True)
    global most_recent
    most_recent = all_individual_folders[0]

def file_copier(handles,localfolder,gridfolder):
    folders = set(in_folder)
    folders = sorted(folders,reverse=True)
    if len(folders) != 0:                                                           # Again the set(A)-set(B) 'trick' - This time, if the set isn't 0, we miss some files...
    #    print('Mismatch found in folder: ' + str(all_files) + '!')                                                                                                                        
#        print('Missing file(s): ' + str(missing_file))                                                                                                                                                 
        for a in folders:                                                                                                                                                             
            if a in most_recent:
                logging.debug('Hey! I found the most recent folder, ' + str(a) + ' - I am skipping this one! (May still be processing/filling as we speak)')
            else:
                location_grid = gridfolder + a + '/'                                                                                                                                           
                location_nikhef = 'file://'+localfolder + a + '/'                                                                                                                             
                subprocess.call(['uberftp','-rm','-r',location_grid])                        # When a file is missing, the folder was either being processed, or the file transfering was interrupted.
                logging.debug('Removed folder: ' + a + '/')                                  # This is why I chose to remove the folder and re-copying it; in case the other files are wrong as well.
                subprocess.call(['uberftp','-mkdir',location_grid])
                subprocess.call(['globus-url-copy', '-r', location_nikhef, location_grid])   # This command then copies the folder back into place
                logging.debug('Copy succesful!')  
        # exit code (exception)
            #checksum adler32
    logging.debug('All files are in place. Moving on . . .')

def copy_ana_cal_files(localfolder,gridfolder):
    logging.debug('Checking all the ANA & CAL files. . .')
    logging.debug('')
    ana_folder = localfolder + 'analysis/'
    grid_ana_folder = gridfolder + 'analysis/'
    grid_cal_folder = gridfolder + 'calibration/'
    cal_folder = localfolder + 'calibration/'
    local_ana = glob.glob1(ana_folder,'*')
    local_cal = glob.glob1(cal_folder,'*')
    the_missing_ana = []
    the_missing_cal = []
    grid_ana = [x for x in which_otherfiles['analysis'] if x.startswith('ANA')]          # This is a list comprehension: in every folder, we check for files ending with .root or .bin!     
    grid_cal = [x for x in which_otherfiles['calibration'] if x.startswith('CAL')]
    missing_ana = list(set(local_ana)-set(grid_ana))
    missing_cal = list(set(local_cal)-set(grid_cal))
    if(len(missing_ana) != 0):
        logging.debug('There are some ANA files missing. Synchronizing...')
        for index in missing_ana:
            path_file = ana_folder + index
            path_grid = grid_ana_folder + index
            subprocess.call(['globus-url-copy','-r',path_file,path_grid])
    if(len(missing_cal) != 0):
        logging.debug('There are some CAL files missing. Synchronizing...')
        for index in missing_cal:
            path_file = cal_folder + index
            path_grid = grid_cal_folder + index
            subprocess.call(['globus-url-copy','-r',path_file,path_grid])
    logging.debug('Finished ANA & CAL file copy.')


def copy_all_missing_folders(localfolder,gridfolder):
    """          copy_all_missing_folders                                                                                                                                                                         WHAT DOES IT DO?                                                                                                                                                                                                           Actually, it's pretty simple. Since we defined not_copied earlier when analyzing local files, we just simply check if the length isn't 0. If it is, great.                                                 If it isn't 0, the copier will correct for the missing folders.  
    """
    if len(not_copied) != 0:
        logging.debug('Number of folder(s) missing: ' + str(len(not_copied)))
        logging.debug('Folder(s) missing: ' + str(not_copied))
    else:
        logging.debug('Have not found any missing folders. All folders are up to date!')
    for tocopy in not_copied:
        if tocopy in most_recent:
            logging.debug('Hey! I found the most recent folder, ' + str(tocopy) + ' - I am skipping this one! (May still be processing/filling as we speak)')
        else:
            logging.debug('Copying folder: ' + tocopy)                                                                                                                                        
            string = 'file://'+localfolder+tocopy+'/'
            subprocess.call(['uberftp','-mkdir',gridfolder+tocopy+'/'])
            subprocess.call(['globus-url-copy', '-r', string, gridfolder+tocopy+'/'])            # Straightforward. This one copies the folders that were missing.
    logging.debug('Finishing copying the folders. Continuing with file comparison')
    logging.debug('-'*60)

def checksums(localfolder,gridfolder):
    user = 'jnobelen'
    adler32_statement_grid = 'curl --head --header \'Want-Digest: ADLER32\' --silent --fail --capath /etc/grid-secutiry/certificates/ --user ' + user + ' ' + checksum_dir 
    list_of_folders = which_files.keys()
    iterable = list_of_folders[:]

 #   print(type(which_files))
  #  print(list_of_folders[:])
    for every in list_of_folders:
        filenames = [x for x in which_files[every]]
#        for a_file in filenames:
#            if 'cal' in a_file:                                                                  # There's an extra folder in the mx_ folders - I will skip this folder for now
 #               print(0)
#            else: 
#                command_subprocess = checksum_dir + every + '/' + a_file
 #               print(command_subprocess)
    return 0
#                print(path_name)
                #checksum_grid = subprocess.Popen(['curl','--head','--header','\'Want-Digest: ADLER32\'','--silent','--fail','--capath','/etc/grid-security/certificates/','--user',user,command_subprocess,' | grep \'adler32=\''],stdout = subprocess.PIPE,stderr=subprocess.PIPE) #--cert --key
                #o,  e = checksum_grid.communicate()
                #print(o)
 #       print(filename)
  #      path = localfolder + filename
#        print(path)
 #       checksum_local = subprocess.Popen(['adler32',path],stdout = subprocess.PIPE,stderr=subprocess.PIPE)
#        o,  e = checksum_local.communicate()     

    
# do_it_all will just run every other 4 functions. Easy!

def do_it_all(handles,localfolder,gridfolder,gridfolder_all):
    exclude_latest_folder(localfolder)
    grid_analyzer(gridfolder_all)
    local_analyzer(localfolder)
    copy_all_missing_folders(localfolder,gridfolder)
    file_analyzer(handles,localfolder,gridfolder)
    file_copier(handles,localfolder,gridfolder)

if __name__ == "__main__":
    main()
