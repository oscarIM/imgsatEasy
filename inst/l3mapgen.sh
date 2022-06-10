#!/bin/bash
#export OCSSWROOT=${OCSSWROOT:-/home/evolecolab/seadas/ocssw}
#source ${OCSSWROOT}/OCSSW_bash.env
for files in *_L3m_tmp.nc; do
  ifile=${files}
  ofile=${files%.*}
  tmp_name=${ofile/_L3m_tmp/_L3mapped}
  ofile=${tmp_name}.nc
  l3mapgen ifile=${ifile} ofile=${ofile} product=${1} oformat=${2} resolution=${3} projection=${4} interp={5} north=${6} south=${7} west=${8} east=${9} quiet=${10} mask_land=${11}
done
rm *_tmp.nc
rm *.x.nc
