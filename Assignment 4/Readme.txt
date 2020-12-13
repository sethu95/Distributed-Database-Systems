Goal:
Two types of spatial data analysis. Hot zone analysis and hot cell/hotspot analysis.
 
Task:
Clone the following repository.
https://github.com/kanchanchy/Hotspot-Analysis-Apache-Sedona-Template
 (Links to an external site.)
Complete all sections where changes are required. Follow the instructions in the repository to build and test your code.
 
Setting Up Apache Spark Test Environment:
1. In order to perform spark-submit with the compiled jar, you have to run Apache Spark on a cluster. You can have three machines or Virtual Machines (One master and two workers). The master should be able to communicate with workers using bi-directional Password-less SSH. Set up your running environment, including installing the Hadoop and Spark. For a step-by-step guide, follow the below PDF:
HDFS_SPark_Setup.pdf
Actions
 
Submission
1. A .jar file that is generated after compiling the source code. We should be able to directly run the .jar file by using ./bin/spark-submit.
2. Full source code. We should be able to compile and generate the .jar file by running 'sbt clean assembly'.
Alert: Do not follow the submission instructions mentioned on the GitHub repository. Create a .zip file combining 1) .jar file and 2) project folder containing source code and .sbt file. The name of the .zip file should be 'CSE512-Assignment4.zip'.  Submit the .zip file.
Note: You need to make sure your code can compile and package by entering sbt clean assembly. We will run the compiled package on our cluster directly using "spark-submit" with parameters. If your code cannot compile and package, you will not receive any points.
 
Input/Output Clarification
In the repository, there are multiple input/output files. It creates confusion like which output file corresponds to which input file. Output file hotcell-example-answer.csv corresponds to the input file yellow_tripdata_2009-01_point.csv. And, output that corresponds to the input file yellow_trip_sample_100000.csv is available at hotcell-example-answer-yellow_trip_sample_100000.csv. I hope it clears all confusions related to input/output in phase 2. 