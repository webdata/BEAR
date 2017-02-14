#!/bin/bash

D_DATA=$1
D_OUT=$2

cmd="bash sort.sh $D_DATA"
echo $cmd
eval $cmd

cmd="python dynstats/diff.py -i $D_DATA/sorted -o D_OUT"
echo $cmd
eval $cmd