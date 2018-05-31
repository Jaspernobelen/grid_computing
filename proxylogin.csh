#!/bin/bash

#Cleaning up an old proxy (if it's there)
rm /user/jaspern/Modulation/gridcomputing/tmp/x509up_u8962
#continue
# Let's source the commands we need to open a proxy!
source /cvmfs/grid.cern.ch/etc/profile.d/setup-cvmfs-ui.sh
export VOMS_USERCONF=$HOME/.glite/vomses
export X509_VOMS_DIR=$HOME/.glite/vomsdir
voms-proxy-init --voms projects.nl:/projects.nl/modulations --valid 168:00
alias python=/project/datagrid/anaconda/bin/python
# Move our tmp proxy file to another place.
#mv /tmp/x509up_u8962 /user/jaspern/Modulation/gridcomputing/tmp/x509up_u8962
echo ' Useful Locations:' 
echo 'gsiftp://gridftp.grid.sara.nl:2811/pnfs/grid.sara.nl/data/projects.nl/modulations/TapeAndDisk/ Processed Data Tape&Disk'
echo 'gsiftp://gridftp.grid.sara.nl:2811/pnfs/grid.sara.nl/data/projects.nl/modulations/Tape/ Raw Data "Tape" '
