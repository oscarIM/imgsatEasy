#!/bin/bash
export OCSSWROOT=${OCSSWROOT:-/home/evolecolab/seadas/ocssw}
source ${OCSSWROOT}/OCSSW_bash.env
l3bin infile=${1} ofile=${2} prod=${3} oformat=${4} verbose=${5} > ${1}"_l3binlog.txt"
