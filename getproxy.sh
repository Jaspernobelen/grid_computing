#!/bin/bash

. /global/ices/lcg/etc/profile.d/grid-env.sh

export MYPROXY_SERVER=px.grid.sara.nl
export X509_USER_PROXY=$HOME/Modulation/gridcomputing/tmp/robot_proxy
export X509_USER_CERT=$HOME/Modulation/gridcomputing/tmp/modulation.crt

cat password | myproxy-get-delegation -S -d -t 25 --voms projects.nl:/projects.nl/modulations
