#!/bin/bash

JAR=/data/compression/BEAR-all-1.0.jar
BASE=/data/compression/dynamic_queries/
for i in s p o 
do
    for c in 0.1 0.2 0.3 0.4 0.6
    do
        java -cp $JAR GetQueries -i $BASE/$i/all.stats -o $BASE/$i -e $c -s 50
    done
    cp $BASE/$i/queries/queries-sel* /data/compression/eval/dynamic/$i/.
done
