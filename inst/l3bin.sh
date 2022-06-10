#!/bin/bash
#export OCSSWROOT=${OCSSWROOT:-/home/evolecolab/seadas/ocssw}
#source ${OCSSWROOT}/OCSSW_bash.env
for files in *_L3b_tmp.nc; do
  infile=${files}
  ofile=${files%.*}
  tmp_name=${ofile/_L3b_tmp/""}
  ofile=${tmp_name}_L3m_tmp.nc
  l3bin infile=${infile} ofile=${ofile} prod=${1} oformat=${2} verbose=${3}
done