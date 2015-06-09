# BEAR
BEAR Benchmark on RDF archives

Queries
==============
We provide the subject, predicate and object lookups we use to feed up our archiving queries (Section 4.2 of our paper)

The structure of the folder is the following:

+ subjectLookup
   - queries-sel-10-e0.2.txt       : Low cardinality and it does not vary by more than a factor of 1 ± 0.2 from the mean
   - queries-sel-100-e0.1.txt      : High cardinality and it does not vary by more than a factor of 1 ± 0.1 from the mean

+ predicateLookup
   - queries-sel-500-e0.6.txt      : Low cardinality and it does not vary by more than a factor of 1 ± 0.6 from the mean
   - queries-sel-1500-e0.6.txt     : High cardinality and it does not vary by more than a factor of 1 ± 0.6 from the mean

+ objectLookup
   - queries-sel-10-e0.1.txt       : Low cardinality and it does not vary by more than a factor of 1 ± 0.1 from the mean
   - queries-sel-100-e0.6.txt      : High cardinality and it does not vary by more than a factor of 1 ± 0.6 from the mean


The format of each query file is the following:

URI MinimumCardinality MeanCardinality MaximumCardinality StandardDeviationCardinality MinDynamicity MeanDinamicity MaxDinamicity StandardDeviationDinamicity
