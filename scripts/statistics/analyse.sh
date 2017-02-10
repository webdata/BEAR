#!/bin/bash

START=1
DIR=/data/compression

DIFF=/data/compression/diffs-renamed
A=$DIR/analysis
mkdir -p $A
F_DATA=$DIR/cleaned-renamed
AWC=$A/data_counts.csv
DWC=$A/diff_counts.csv
DLS=$A/data_size.dat
DIFFLS=$A/diff_size.dat
D_G=$A/data_growth.csv
DYN=$A/dynamicity.csv

for i in `seq 1 58`
do
    echo "Count statements in $i"
    c=`zcat $F_DATA/$i.nt.gz | wc -l`
    cs=`du -m $F_DATA/$i.nt.gz`
    echo "$i,$c" >> $AWC
    echo "$i $cs" >> $DLS

    let p=$i-1
    dc=`zcat $DIFF/v$p-$i.diff.gz |wc -l`
    ds=`du -m $DIFF/v$p-$i.diff.gz`
    echo "$p $ds" >> $DIFFLS
    addc=`zcat $DIFF/v$p-$i.diff.gz | grep "^>" | wc -l`
    delc=`zcat $DIFF/v$p-$i.diff.gz | grep "^<" | wc -l`
    echo "Count diff betwen $p and $i = $DIFF/v$p-$i.diff.gz = $dc"
    echo "Count add diff betwen $p and $i = $DIFF/v$p-$i.diff.gz = $addc"
    echo "Count del diff betwen $p and $i = $DIFF/v$p-$i.diff.gz = $delc"
    echo "$p,$p-$i,$dc,$addc,$delc">>$DWC

    let sum=$c+$p_c
    dyn=`echo "$dc/$sum"|bc -l`
    dadd=`echo "$addc/$p_c"|bc -l`
    ddel=`echo "$delc/$p_c"|bc -l`
    echo "compute dynamicity: $dc/($c+$p_c =$sum) = $dyn"
    echo "compute dynamicity: $addc/($p_c) = $dadd"
    echo "compute dynamicity: $delc/($p_c) = $ddel"
    echo "$i,$dyn,$dadd,$ddel" >> $DYN


    dg=`echo "$c/$p_c"|bc -l`
    echo "Compute growth: $c/$p_c = $dg"
    echo "$i,$dg">>$D_G

    p_c=$c
done
#let START=NO-BINS
