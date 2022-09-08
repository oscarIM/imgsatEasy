#!/bin/bash
export OCSSWROOT=${OCSSWROOT:-/home/evolecolab/seadas/ocssw}
source ${OCSSWROOT}/OCSSW_bash.env
l2bin infile=${1} ofile=${2} prodtype=${3} l3bprod=${4} resolve=${5} verbose=${6} flaguse=${7} qual_max=${8} latnorth=${9} latsouth=${10} loneast=${11} lonwest=${12} minobs=${13} area_weighting=${14} > ${1}"_l2binlog.txt"
