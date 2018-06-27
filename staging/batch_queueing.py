#!/usr/bin/python2.7 
"""                                                                                                                                                                                                         
                                                                                                                                                                                                            
                          Documentation of determine_the_files.py                                                                                                                                          

              WRITTEN BY: J.C.P.Y. Nobelen - v1 03-26-2018
                                             v2 05-16-2018
              CONTACT: jaspern@nikhef.nl                                                                                                                                                                                
              You, the user, will give a folder to the script. It will check whether the files in this folder are online & ready (staged) or require staging.
              If they do - the script will call a staging request 
                                                                                                                                                                                                            
                                                                                                                                                                                                         
                            How to call the script: Copy_all -i <Directory>                                                                                                                                                                                                            
             ARGUMENTS                                                                                                                                                                                     

             -i: Which files would I like to stage?                      You can answer with anything, as long as it is a valid name. 
                                                                         Example: mx_n_201610
                                                                         It will now stage all files which include 201610 - all folders of October 2016.
             CUSTOMISATION                                                                                                                                                                                  
             You can change the input folders of the script to whatever you'd like. These are the only variables to change in the script.                                                                   
                                                                                                                                                                                                            
"""
# source z37_setup_stoomboot.csh before this script
# source modexp_envs.csh before this script - make sure that 'tmp_folder' is inDir in the modexp environment!!
import pythonpath
import time
from time import sleep
import re
import sys
import string
#from string import strip
import gfal
import pprint
import subprocess
from subprocess import check_output
import re
import os,glob,sys
import argparse
import logging, datetime, time

username = "jaspern"

os.system('export X509_USER_PROXY=/user/jaspern/Modulation/gridcomputing/tmp/x509up_u8962')
BASE_DIR_STAGING = "/user/jaspern/Modulation/gridcomputing/staging/"
BASE_DIR_GRIDCOMPUTING = "/user/jaspern/Modulation/gridcomputing/"
inputgrid_dir = "gsiftp://gridftp.grid.sara.nl:2811/pnfs/grid.sara.nl/data/projects.nl/modulations/Tape/tar/*"              # Where should I copy RAW data to?                                                                                    
grid_folder = "gsiftp://gridftp.grid.sara.nl:2811/pnfs/grid.sara.nl/data/projects.nl/modulations/Tape/tar/"              
where_to_store_file = BASE_DIR_STAGING + "datasets/"
statescript = BASE_DIR_STAGING + "state.py"
stagingscript = BASE_DIR_STAGING + "stage.py"
copyallscript = BASE_DIR_GRIDCOMPUTING + "Copy_all.py"

stoomboot_basedir = "/user/jaspern/Modulation/stoomboot/"
stoomboot_submitscript = stoomboot_basedir + "/stoomboot_submit.py"
setup_stoomboot = "/user/jaspern/z37_setup_stoomboot.csh"


fname = "paths.txt"
save_in_files = 'files'
#tmp_folder = "/user/jaspern/Modulation/gridcomputing/staging/temptest/"
tmp_folder = "/data/modulation/Raw_Data/tmp_sara/"

starting_year = 2015
number_of_years = 15

#grid_analyzer(inputgrid_dir)

def main():
    parser = argparse.ArgumentParser(description='Determine which files need to be staged.')
    parser.add_argument('-i', '--input', help="Which folder do you want to have copied?",nargs='?',const='mx_n',default = 'mx_n')
    args = parser.parse_args()
    global inDir
    inDir = args.input
    if inDir == 'mx_n': #Batch process activated 
        BatchOn = 1
    else: BatchOn = 0
#    subprocess.call(['/project/datagrid/anaconda/bin/python', copyallscript,'-i','1','-p','0'])
    #save_output = open(fname,"w")
#    queue_checker()

    year_and_month = []
    months_of_year = ['01','02','03','04','05','06','07','08','09','10','11','12']
    for j in range(number_of_years):                                             #
        years_new = starting_year+j
        for i in range(len(months_of_year)):
            year_and_month.append(str(years_new)+months_of_year[i])
            
    for k in range(len(year_and_month)):
        queue_checker()
        save_output = open(fname,"w")
        grid_analyzer(inputgrid_dir,year_and_month[k])
        if len(path_fields) != 0:
            for path_name in path_fields:
                save_output.write(path_name+'\n')
            save_output.close()
            final_location = where_to_store_file + fname
            subprocess.call(['mv',fname,final_location])
            os.system('grep --only-matching ' + '/pnfs/grid.sara.nl.* ' + final_location + ' > ' + save_in_files)
            subprocess.call(['/project/datagrid/anaconda/bin/python',statescript])
            subprocess.call(['/project/datagrid/anaconda/bin/python',stagingscript])
            createspace()
            synchronizer()
            submitter_to_stoomboot()

#This part finally creates a file with strings that the staging script uses    
#    os.system('grep --only-matching ' + '/pnfs/grid.sara.nl.* ' + final_location + ' > ' + save_in_files)
#    subprocess.call(['/project/datagrid/anaconda/bin/python',statescript])
#    subprocess.call(['/project/datagrid/anaconda/bin/python',stagingscript])
# submit staging, cr
# Create our temporary files
 #   createspace()
    #put a check whether everything went OK - run state script, grep all the NEARLINE files, if len == 0, continue
# make it online for a certain period of time (staging script) 
#    synchronizer()

def grid_analyzer(gridfolder_all,handle):
    outputftp = subprocess.Popen(['uberftp','-ls',gridfolder_all],stdout = subprocess.PIPE,
                          stderr=subprocess.PIPE)
    o,  e = outputftp.communicate()                                                          # This creates our output. We will analyze the strings that come out of the uberftp -ls command.                                                                     
    lines = o.split('\n')                                                                    # Splitting every line                                                                                                                                          
    global path_fields
    path_fields = []
    global file_name
    file_name = []
    global list_of_folders
    list_of_folders = {}
    for line in lines:
        ls_fields = line.split()
        if len(ls_fields) <= 1:                                                              # Our output is like this: ['A', 'B', 'C', 'D', 'folder', 'file']                              
            continue                                                                         # Not all lines consist of information we need. We need at least the file + folder, so anything <1 is wrong.  
        path_name = ls_fields[-1].split('/')
        if handle in path_name[-1]:
            file_name.append(path_name[-1])
            path_fields.append(ls_fields[-1])
#    print(file_name)
    return path_fields,file_name


def createspace():
#    print(list_of_folders)
    if os.path.exists(tmp_folder):
        subprocess.call(['rm','-r',tmp_folder])                                              # First check: is there a folder called like the one we gave?
    subprocess.call(['mkdir',tmp_folder])
    folder_names = list_of_folders.keys()

    for every in folder_names:                                                               # Here I create the paths to the temporary files
#        print('Copying folder: ' + every)
        temporary_folder_path = tmp_folder + every + '/' 
#        print(temporary_folder_path)
        if os.path.exists(temporary_folder_path):
            subprocess.call(['rm','-r',temporary_folder_path])                               # Double check; if something went wrong, remove the existing temporary folder.
        subprocess.call(['mkdir',temporary_folder_path])

def state_of_files():
    statescript_output = subprocess.Popen(['/project/datagrid/anaconda/bin/python',statescript],stdout = subprocess.PIPE,
                          stderr=subprocess.PIPE)
    o,e = statescript_output.communicate()
    lines = o.split('\n')
    global paths_checked
    paths_checked = []
    state_of_paths = []
    folder_names = []
    tarfiles = []
    for line in lines:
        all_lines = line.split()
        if len(all_lines) < 1:
            continue
        paths_checked.append(all_lines[0])
        tarfiles.append(all_lines[0].split('/')[-1])
        state_of_paths.append(all_lines[1])
    return tarfiles, state_of_paths

def synchronizer():
    tar_files, state_of_paths = state_of_files()
    a = 0
    print(len(paths_checked))
    while a < len(paths_checked):
        tarfiles, state_of_paths = state_of_files()
        for i in range(len(paths_checked)):
            if 'ONLINE' in state_of_paths[i]:
                print('Copying file: ' + tarfiles[i])
                local_path_name = tmp_folder + tarfiles[i]
                grid_path_name = grid_folder + tarfiles[i]
                new_folder_name = tarfiles[i].split('.')[0]
                target_directory = tmp_folder + new_folder_name + '/'
                cmd = 'tar -xvf ' + local_path_name + ' -C ' + target_directory + ' .' 
                subprocess.call(['globus-url-copy','-r',grid_path_name,local_path_name])
                subprocess.call(['mkdir',target_directory])
                subprocess.call(['tar','-xvzf',local_path_name,'-C',target_directory,'.'])
                a=a+1
            else:
                a=a+0 
                sleep(5)

def queue_checker():

    number_of_queued_jobs  = "qselect -u " + username + " -s Q | wc -l"
    number_of_running_jobs = "qselect -u " + username + " -s R | wc -l"

    is_done_r = int(subprocess.check_output(number_of_running_jobs,shell=True))
    is_done_q = int(subprocess.check_output(number_of_queued_jobs,shell=True))
    
    ready = True
    while ready == True:
         number_of_queued_jobs  = "qselect -u " + username + " -s Q | wc -l"
         number_of_running_jobs = "qselect -u " + username + " -s R | wc -l"

         is_done_r = int(subprocess.check_output(number_of_running_jobs,shell=True))
         is_done_q = int(subprocess.check_output(number_of_queued_jobs,shell=True))
         if (is_done_r == 0) and (is_done_q == 0):
             ready = False
         else: 
             print('There are currently files being processed...')
             sleep(120)

def submitter_to_stoomboot():
    print('Hello_2')
    basefolder_rawdata = tmp_folder
    all_files = glob.glob1(tmp_folder,'mx_*')
    only_folders = []
    for i in range(len(all_files)):
        if all_files[i].endswith('.tar'):
            continue
        else: only_folders.append(all_files[i])
    
    for i in range(len(only_folders)):
        folder_location = tmp_folder + only_folders[i] + '/'
        os.system('cd ' + stoomboot_basedir)
        subprocess.call(['python',stoomboot_submitscript, '-i', folder_location, '-p', '0'])
if __name__ == "__main__":
    main()
