#!/bin/bash
export OCSSWROOT=${OCSSWROOT:-/home/evolecolab/seadas/ocssw}
source ${OCSSWROOT}/OCSSW_bash.env
for files in *.nc; do
	infile=${files}
	name_file=${infile%%.*}
	ofile=${name_file}_${1}_${2}_${3}Km_L3b_tmp.nc
l2bin infile=${infile} ofile=${ofile} prodtype=${1} l3bprod=${2} resolve=${3} verbose=${4} flaguse=${5} qual_max=${6}
done
