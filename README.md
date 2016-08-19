# Hadoop MR2 Jobs Traces  
##Description 
Standard way to do performance comparison is to execute the same workload on the systems being measured, compute potentially multiple performance metrics from the observed behaviour, then assess the pros and cons of each system.
SWIM allows realistic and representative workloads to be used to drive such performance comparisons.



###What is SWIM ?
Statistical Workload Injector for MapReduce 
This SWIM supoprts for MR1 

Please refer the link for SWIM and how to use 
https://github.com/SWIMProjectUCB/SWIM/wiki



###Why SWIM-2 ? , Why not SWIM ? 
SWIM Supports only MR1 but SWIM-2 Supports MR2 

###Code Changes 
In SWIM to parse the hadoop job logs they pointed the dir location to the script, But in MR2 we are moved the job history directory in HDFS. So, we cannot point parse script to HDFS and also not by downloading from HDFS because it will be sequence file.
Finally we replaced the parse hadoop jobs script by creating the python script which will parse the job details from History Server 



###How to use this code

This code generally replace the one of the perl script in swim.
Swim use perl to fetch the job trace from hadoop job history dir for MR1.
In MR2 we moved  the jobs logs to hdfs,so pointing to the history server this python script will pull the job traces and store the ouput in tsv

Code replaced parse-hadoop-jobhistory.pl as parse-hadoop-jobhistory.py


for code : https://github.com/SWIMProjectUCB/SWIM/blob/master/workloadSuite/parse-hadoop-jobhistory.pl
for doc :  https://github.com/SWIMProjectUCB/SWIM/wiki/Analyze-historical-cluster-traces-and-synthesize-representative-workload



After Python Ouput
Use the SWIM WorkloadSynthesis.pl
refer https://github.com/SWIMProjectUCB/SWIM/wiki



Working on SWIM :
Please refer the SWIM page for how to use swim 
instead of parse-hadoop-jobhistory.pl 
download parse-hadoop-jobhistory.py python scipt to support for MR2



