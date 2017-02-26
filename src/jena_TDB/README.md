# Compile
- mvn install

# run Queries

 - java -cp target/tdbQuery-0.5-jar-with-dependencies.jar org.ai.wu.ac.at.tdbArchive.tools.JenaTDBArchive_query

##usage:
 -a,--allVersionQueries <arg>   dynamic queries to process in all versions
 
 -c,--category <arg>            Query category: mat | diff | ver | change
 
 -d,--dir <arg>                 DIR to load TDBs
 
 -e,--endversion <arg>          Version end, used in the Query (e.g. in
                                diff)
                                
 -h,--help                      Shows help
 
 -j,--jump <arg>                Jump step for the diff: e.g. 5
                                (0-5,0-10..)
                                
 -o,--OutputResults <arg>       Output file with Results
 
 -p,--policy <arg>              Policy implementation: ic | cb | tb | cbtb
                                | hybrid
                                
 -q,--query <arg>               single SPARQL query to process, applied on
                                -v version
                                
 -Q,--MultipleQueries <arg>     file with several SPARQL queries
 
 -r,--rol <arg>                 Rol of the Resource in the query: subject
                                (s) | predicate (p) | object (o)
                                
 -s,--silent                    Silent output, that is, don't show results
 
 -t,--timeOutput <arg>          file to write the time output
 
 -v,--version <arg>             Version, used in the Query (e.g. in
                                materialize)


#Sample queries from the subjects, predicates or objects
 - java -cp build/libs/BEAR-all-1.0.jar GetQueries
 
##usage:
 
-e <arg>    epsilon value
 
-h,--help   show help.

-i <arg>    the statistic file
  
-o <arg>    output directory
 
-s <arg>    number of maximum URLs
