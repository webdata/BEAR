#!/bin/bash

# script input
D_DATA=$1
F_LOGS=$2

##########
#data folder
##########
F_TMP=$D_DATA/sort.tmp
echo "Creating tmp sorting directory $F_TMP"
mkdir -p $F_TMP

D_DATA_SORTED=$D_DATA/sorted
echo "Creating tmp directory $D_DATA_SORTED"
mkdir -p $D_DATA_SORTED


##########
#LOGS
##########
echo "Genrating log directory $F_LOGS"
mkdir -p $F_LOGS

SB=$F_LOGS/sorting.bench
echo "Sortign benchmarks are at $SB"

cd $D_DATA
for DATA in `ls  *.nt`
do

    echo "Sorting $DATA to $D_DATA_SORTED/$DATA"

    rm -rf $F_TMP/*

	START=`date +%s`
	cmd="cat $DATA | sort -T $F_TMP -u   > $D_DATA_SORTED/$DATA"
	echo $cmd
	eval $cmd
	END=`date +%s`
	ELAPSED=`echo "scale=8; ($END - $START) / 1000000000" | bc`
	echo "$DATA $ELAPSED sec" >> $SB
done
