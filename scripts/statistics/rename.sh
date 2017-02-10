#!/bin/bash
dir=/data/compression/tdb

for i in `seq 42 60`
do
    pos=$(($i-2))
    a=$dir/$i.tdb
    b=$dir/$pos.tdb
    echo "Moving $a to $b"
    mv $a $b
done
