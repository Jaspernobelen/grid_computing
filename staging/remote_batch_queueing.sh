#!/bin/bash

screen -d -m bash -c 'source /cvmfs/grid.cern.ch/etc/profile.d/setup-cvmfs-ui.sh && export X509_VOMS_DIR=$HOME/.glite/vomsdir && export VOMS_USERCONF=$HOME/.glite/vomses && export X509_USER_PROXY=/user/jaspern/Modulation/gridcomputing/tmp/x509up_u8962 && /project/datagrid/anaconda/bin/python /user/jaspern/Modulation/gridcomputing/staging/batch_queueing.py'