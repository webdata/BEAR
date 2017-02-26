#!/bin/sh

# script input
D_DATA=$1

##########
#data folder
##########
cd $D_DATA
F_TMP=sort.tmp
echo "Creating tmp sorting directory $F_TMP"
mkdir -p $F_TMP

D_DATA_SORTED=sorted
echo "Creating tmp directory $D_DATA_SORTED"
mkdir -p $D_DATA_SORTED


#subject, object, predicate extraction
# taken from http://blog.datagraph.org/2010/03/grepping-ntriples
alias rdf-subjects="awk '/^\s*[^#]/ { print \$1 }' | uniq"
alias rdf-predicates="awk '/^\s*[^#]/ { print \$2 }' | uniq"
alias rdf-objects="awk '/^\s*[^#]/ { ORS=\"\"; for (i=3;i<=NF-1;i++) print \$i \" \"; print \"\n\" }' | uniq"


for DATA in `ls *.nt*`
do
    if [[ $DATA =~ \.gz$ ]];
    then
        echo "gzip input"
        cmdcut="zcat $DATA"
        suffix=""
    else
        cmdcut="cat $DATA"
        suffix=".gz"
    fi

    rm -rf $D_DATA_SORTED/*$DATA*
    echo "Sorting $DATA to $D_DATA_SORTED"

    rm -rf $F_TMP/*

	START=`date +%s`
	cmdsort="sort -T $F_TMP -u |gzip -c > $D_DATA_SORTED/data-$DATA$suffix"
	cmdsubj="tee >(awk '/^\s*[^#]/ { print \$1 }' |gzip -c > $D_DATA_SORTED/subj-$DATA$suffix)"
	cmdpred="tee >(awk '/^\s*[^#]/ { print \$2 }' |gzip -c > $D_DATA_SORTED/pred-$DATA$suffix)"
	cmdobj="tee >(awk '/^\s*[^#]/ { ORS=\"\"; for (i=3;i<=NF-1;i++) print \$i \" \"; print \"\n\" }' |gzip -c > $D_DATA_SORTED/obj-$DATA$suffix)"



	cmd="$cmdcut |  $cmdsubj | $cmdpred| $cmdobj| $cmdsort"

    echo $cmd
	eval $cmd
	END=`date +%s`
	ELAPSED=`echo "scale=8; ($END - $START) / 1000000000" | bc`
	echo "$DATA $ELAPSED sec"

    echo "Sorting vocab files for $DATA"
    for a in subj pred obj
    do
        cmd="gunzip -c $D_DATA_SORTED/$a-$DATA$suffix | sort -T $F_TMP -u |gzip -c> $D_DATA_SORTED/$a-$DATA.sorted.gz"
        echo $cmd
        eval $cmd
        mv $D_DATA_SORTED/$a-$DATA.sorted.gz $D_DATA_SORTED/$a-$DATA$suffix
    done
done
