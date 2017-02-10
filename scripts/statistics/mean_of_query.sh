#!/bin/bash

m_c=`awk '{ sum += $2 } END { if (NR > 0) print sum / NR }' $1`
m_d=`awk '{ sum += $7 } END { if (NR > 0) print sum / NR }' $1`
n=`cat $1 | wc -l`
echo "$1 $m_c $m_d $n"
