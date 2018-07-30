# This is a simple program made to demonstrate Map Side Join ->

## Setting up :

1. First you need to put the files inside the input folder onto hdfs file system
2. After this you need to type the command to rn the jar file already generated.

#### Commands ->
<b> To put file onto hdfs - </b>
``` 
hdfs dfs put <filename> 
```

<b> To run the jar file </b>
```
 hadoop jar <jarfilename> <mainclass> <input> <output> 
 hadoop jar MapSideJoin MapSideJoin transactions output
```

#### Dependencies ->
<b> Hadoop 2</b>
