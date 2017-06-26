# Compile

Please compile and execute the tools with the HDT-C++ library: https://github.com/rdfhdt/hdt-cpp

# Run Queries

 - java  -cp target/tdbQuery-0.6-jar-with-dependencies.jar org.ai.wu.ac.at.tdbArchive.tools.JenaTDBArchive_query
 
Please consider to increase the Java heap with the flag *-XmxSIZE*, e.g. "java *-Xmx64G* -cp ..." to use a maximum of 64GB RAM memory.

## Usage:

| Argument      | Result      |
| ------------- |-------------|
| -d `<arg>`              | DIR to load the HDT versions|
| -h                      |Shows help|
| -i `<arg>`              | Input SPARQL query to process|
| -l `<arg>`              |Limit up to the *number* of versions|
| -o `<arg>`              | Output file with Results|
| -t `<arg>`              | Type of query: subject (s) &#124; predicate (p) &#124; object (o)|