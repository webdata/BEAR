#!/bin/sh

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


#subject, object, predicate extraction
# taken from http://blog.datagraph.org/2010/03/grepping-ntriples
alias rdf-subjects="awk '/^\s*[^#]/ { print \$1 }' | uniq"
alias rdf-predicates="awk '/^\s*[^#]/ { print \$2 }' | uniq"
alias rdf-objects="awk '/^\s*[^#]/ { ORS=\"\"; for (i=3;i<=NF-1;i++) print \$i \" \"; print \"\n\" }' | uniq"

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
    rm -rf $D_DATA_SORTED/*$DATA*
    echo "Sorting $DATA to $D_DATA_SORTED"

    rm -rf $F_TMP/*

	START=`date +%s`
	cmdsort="sort -T $F_TMP -u   > $D_DATA_SORTED/data-$DATA"
	cmdsubj="tee >(awk '/^\s*[^#]/ { print \$1 }' >$D_DATA_SORTED/subj-$DATA)"
	cmdpred="tee >(awk '/^\s*[^#]/ { print \$2 }' >$D_DATA_SORTED/pred-$DATA)"
	cmdobj="tee >(awk '/^\s*[^#]/ { ORS=\"\"; for (i=3;i<=NF-1;i++) print \$i \" \"; print \"\n\" }' >$D_DATA_SORTED/obj-$DATA)"
	cmd="cat $D_DATA/$DATA |  $cmdsubj | $cmdpred| $cmdobj| $cmdsort"

    echo $cmd
	eval $cmd
	END=`date +%s`
	ELAPSED=`echo "scale=8; ($END - $START) / 1000000000" | bc`
	echo "$DATA $ELAPSED sec" >> $SB

    echo "Sorting vocab files for $DATA"
    for a in subj pred obj
    do
        cmd="cat $D_DATA_SORTED/$a-$DATA | sort -T $F_TMP -u > $D_DATA_SORTED/$a-$DATA.sorted"
        echo $cmd
        eval $cmd
        mv $D_DATA_SORTED/$a-$DATA.sorted $D_DATA_SORTED/$a-$DATA
    done
done
