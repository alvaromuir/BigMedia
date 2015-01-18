#!/bin/bash
# Script to prep a downloaded dfa file for import
# takes a smample
# and gzips for export
# @alvaromuir, 12.31.14

set -e
USAGECMD="Usage: $0 <file> <opt: outputname> <opt: sample_outputname>"
if [ -z "$1" ]
  then
  echo "ERROR - No dfa report supplied. "
  echo $USAGECMD
  exit
else
  if [ ! -e "$1" ]
    then
    echo "ERROR - $1 not found. Please check path and file name."
  else
    output=""
    sampleoutput=""
    if [ -z "$2"]
      then
      output="prepped_dfa_file"
    else
      output=$2
    fi
    if [ -z "$3"]
      then
      sampleoutput=$output"_sample"
    else
      sampleoutput=$3
    fi
    echo ""
    echo " ... preping "${1##*/}" ..."
    echo "removing header and footer rows ..."
    sed 1,10d $1 | sed '$d' > $output.csv
    echo "done!"
    echo "sampling data ..."
    subsample -n 750 $output.csv >> $sampleoutput.csv
    echo "done!"
    echo "gzipping file ..."
    gzip $output.csv
    echo "done !"
    echo ""
    echo "Your prep'd file is $output.csv.gz, and your sample is $sampleoutput.csv"
  fi
fi
