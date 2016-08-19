What is SWIM ?
Statistical Workload Injector for MapReduce 
https://github.com/SWIMProjectUCB/SWIM/wiki

Why SWIM-2 ? , Why not SWIM ? 
SWIM Supports only MR1 but SWIM-2 Supports MR2 
Code Changes 
In SWIM to parse the hadoop job logs they pointed the dir location to the script, But in MR2 we are moved the job history directory in HDFS. So, we cannot point parse script to HDFS and also not by downloading from HDFS because it will be sequence file.
Finally we replaced the parse hadoop jobs script by creating the python script which will parse the job details from History Server 
