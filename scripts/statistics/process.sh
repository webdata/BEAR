#!/bin/bash
NO=$1
BINS=10
let START=NO-BINS
echo "Processing $START to $NO"


##########
#data folder
##########
F_IN=/data/swse.deri.org/dyldo/data
LIB=/data/compression/BEAR-all-1.0.jar
F_TMP=/data/compression/tmp.sort-$NO
tdb=/home/jumbrich/apache-jena-2.12.1/bin/tdbloader2


##########
#output folders
##########
F_RAW=/data/compression/raw
F_DATA=/data/compression/cleaned
F_TDB=/data/compression/tdb
F_LOG=/data/compression/logs-$NO


echo "Preparing setup"
for i in  F_DATA F_TDB F_LOG F_TMP F_RAW
do
	echo ${!i}
	mkdir -p ${!i}
done



##########
#LOGS
##########
DID=$F_LOG/data_id.log
PB=$F_LOG/parser.bench
SB=$F_LOG/sorting.bench
TB=$F_LOG/tdb.bench
DB=$F_LOG/diff.bench

let CNT=START+1


cd $F_IN
for v in $(ls -d */ | head -n $NO | tail -n $BINS)
do
	v=${v%%/}
	FROM=$F_IN/$v/data.nq.gz
	RAW_DATA=$F_RAW/$CNT.nt.gz
	DATA=$F_DATA/$CNT.nt.gz
	
	PO=$F_LOG/parse.$v.out
	PE=$F_LOG/parse.$v.err
	echo "$v -> $CNT" >> $DID

	echo "Parsing $FROM to $RAW_DATA"
	
	START=`date +%s%N`
	cmd="java -jar $LIB $FROM $RAW_DATA  >$PO 2>$PE"
	echo $cmd
	eval $cmd
	END=`date +%s%N`
	ELAPSED=`echo "scale=8; ($END - $START) / 1000000000" | bc`
	echo "$v $ELAPSED sec" >> $PB

	echo "Sorting $RAW_DATA"
	
	START=`date +%s%N`
	cmd="zcat $RAW_DATA | sort -T $F_TMP -u | gzip -c  > $DATA"
	echo $cmd
	eval $cmd
	END=`date +%s%N`
	ELAPSED=`echo "scale=8; ($END - $START) / 1000000000" | bc`
	echo "$v $ELAPSED sec" >> $SB

	TDB=$F_TDB/$CNT.tdb
	mkdir $TDB
	
	TDBO=$F_LOG/tdb.$v.out
	TDBE=$F_LOG/tdb.$v.err
	echo "Building TDB index"
	START=`date +%s%N`
	cmd="zcat $DATA | sh $tdb --loc $TDB 1>$TDBO 2>$TDBE"
	echo $cmd
	eval $cmd
	END=`date +%s%N`
	ELAPSED=`echo "scale=8; ($END - $START) / 1000000000" | bc`
	echo "$v $ELAPSED sec" >> $TB
	
	rm $F_TMP/*
	
	let CNT=CNT+1
	
done

rm -rf $F_TMP

DIFF=/data/compression/diffs
mkdir -p $DIFF

let S=NO-BINS
let Z=NO-1
if [ $S -eq 0 ]; then
	let S=S+1
fi

for i in `seq $S $Z`;
do
	vone=$F_DATA/$i.nt.gz
	a=$((i+1))
	vtwo=$F_DATA/$a.nt.gz
	echo "diff $vone $vtwo"

	START=`date +%s%N`
	cmd="diff <(zcat $vone) <(zcat $vtwo) | gzip -c > $DIFF/v$i-$a.diff.gz"
	eval $cmd	
	END=`date +%s%N`
	ELAPSED=`echo "scale=8; ($END - $START) / 1000000000" | bc`
	echo "$i $ELAPSED sec" >> $DB
done
