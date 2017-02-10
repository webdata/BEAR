#!/bin/bash

java -Xmx100G -cp BEAR-all-1.0.jar AnalyseRes -d /data/compression/tdb -f /data/compression/analysis/predicatesModifiedInSnapshots.gz -o /data/compression/dynamic_queries -t p 1> p.out 2>p.err

d=subjectsModifiedInSnapshots.gz
java -Xmx100G -cp BEAR-all-1.0.jar AnalyseRes -d /data/compression/tdb -f /data/compression/analysis/$d -o /data/compression/dynamic_queries -t s 1> s.out 2>s.err

d=objectsModifiedInSnapshots.gz
java -Xmx100G -cp BEAR-all-1.0.jar AnalyseRes -d /data/compression/tdb -f /data/compression/analysis/$d -o /data/compression/dynamic_queries -t o 1> o.out 2>o.err
