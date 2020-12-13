I installed Hadoop on Windows 10 and generated jar files using Eclipse IDE after importing the external JAR archives.

The command to run the jar file is as follows:
hadoop jar equijoin.jar equijoin <input file path> <output directory path>

It takes 2 inputs in args[], the first is the input path, second is the output path.

Driver
A job is created with parameters for OutPutKey, OutPutValue, and also the Mapper and Reducer classes (in the same file). Key values are generated from the reducer. Parameters are also set for input and output files from arguments. args[0] is the input and args[1] is the output. The job executes until it completes.

Mapper
Mapper is called first and the input file is passed as input and it processes rows or tuples in the file. Columns or fields are also iterated. Second column is the join column value. It is used as key and the row is used as value. 

Reducer
Reducer is called last and the key and list of values are taken as input. Reducer separates the rows related to table 1 and table 2. It puts them in 2 different ArrayLists. Finally the combinations of the two lists are generated and written to the final text as output. 