#!/bin/bash
export OCSSWROOT=${OCSSWROOT:-/home/evolecolab/seadas/ocssw}
source ${OCSSWROOT}/OCSSW_bash.env
  l3mapgen ifile=${1} ofile=${2} product=${3} oformat=${4} resolution=${5} projection=${6} interp=${7} north=${8} south=${9} west=${10} east=${11} quiet=${12} mask_land=${13}
mkdir log_files
mv *.txt log_files

