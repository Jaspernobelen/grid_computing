#!/bin/bash

source /cvmfs/grid.cern.ch/etc/profile.d/setup-cvmfs-ui.sh
echo 'Setting up the grid. . .'
mkdir -p $HOME/.glite/vomsdir/projects.nl

cat > $HOME/.glite/vomsdir/projects.nl/voms.grid.sara.nl.lsc <<EOF                                                                                                                             
/O=dutchgrid/O=hosts/OU=sara.nl/CN=voms.grid.sara.nl                                                                                                                                                       
/C=NL/O=NIKHEF/CN=NIKHEF medium-security certification auth                                                                                                                                                
EOF

mkdir -p $HOME/.glite/vomses
cat > $HOME/.glite/vomses/projects.nl-voms.grid.sara.nl <<EOF                                                                                                                                  
"projects.nl" "voms.grid.sara.nl" "30028" "/O=dutchgrid/O=hosts/OU=sara.nl/CN=voms.grid.sara.nl" "projects.nl"                                                                                             
EOF

export X509_VOMS_DIR=$HOME/.glite/vomsdir
export VOMS_USERCONF=$HOME/.glite/vomses
voms-proxy-init --voms projects.nl:/projects.nl/modulations