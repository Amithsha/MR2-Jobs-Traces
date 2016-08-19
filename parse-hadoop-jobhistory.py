#!/usr/bin/python

import sys
sys.path.append('/home/amithsha.s/python')
import requests
import getopt
import datetime
import time
import os
import json 
from datetime import timedelta  
from optparse import OptionParser





##START THE JOB 

def start_jobs(url):
	url =  url
	jobs=requests.get('http://%s'%url , headers = {'ACCEPT':'application/json'}).json().get('jobs')   
	jobs = jobs['job']
	get_job(url,jobs)

job_inf_dict={}

##GET THE COUNTER INFORMATION 

def job_counter_process(xml_input,id,mili_date):
	import sys
	import ast	
	import subprocess
	id=id
	mili_date=mili_date
	job = xml_input
	counter={}
	try:
		for i in  job['counterGroup']:
        	        for x in i['counter']:
				counter =ast.literal_eval(json.dumps(x))
				if counter['name'] == 'HDFS_BYTES_READ' :
					job_inf_dict['map_input_bytes']=counter['totalCounterValue']

				if counter['name'] == 'HDFS_BYTES_WRITTEN' :
					job_inf_dict['reduce_output_bytes']=counter['totalCounterValue']
				if counter['name'] == 'REDUCE_SHUFFLE_BYTES' :
					job_inf_dict['SHUFFLE_BYTES']=counter['totalCounterValue']



	except Exception,x:
		pass	




##GET THE JOB INFORMAATION 

def job_inf_process(inf_input,mili_date):
	import sys
	mili_date=mili_date
	job_inf = inf_input

	submitTime		=job_inf['submitTime']
	startTime		=job_inf['startTime']
	finishTime		=job_inf['finishTime']
	id			=job_inf['id']
	state			=job_inf['state']
	mapsTotal		=job_inf['mapsTotal']
	mapsCompleted		=job_inf['mapsCompleted']	
	reducesTotal		=job_inf['reducesTotal']
	reducesCompleted	=job_inf['reducesCompleted']
	avgMapTime		=job_inf['avgMapTime']	
	avgReduceTime		=job_inf['avgReduceTime']
	avgShuffleTime		=job_inf['avgShuffleTime']
	
	job_inf_dict['submitTime']=submitTime  

	sub_time = datetime.datetime.fromtimestamp(submitTime/1000.0).strftime('%Y-%m-%d %H:%M:%S')
	str_time = datetime.datetime.fromtimestamp(startTime/1000.0).strftime('%Y-%m-%d %H:%M:%S')
	fns_time = datetime.datetime.fromtimestamp(finishTime/1000.0).strftime('%Y-%m-%d %H:%M:%S')
	initialDelay = ((datetime.datetime.strptime(str_time,'%Y-%m-%d %H:%M:%S')) -  (datetime.datetime.strptime(sub_time,'%Y-%m-%d %H:%M:%S'))).total_seconds()/60
	elapsedTime = ((datetime.datetime.strptime(fns_time,'%Y-%m-%d %H:%M:%S')) -  (datetime.datetime.strptime(str_time,'%Y-%m-%d %H:%M:%S'))).total_seconds()/60
	actualelapsedTime = ((datetime.datetime.strptime(fns_time,'%Y-%m-%d %H:%M:%S')) -  (datetime.datetime.strptime(sub_time,'%Y-%m-%d %H:%M:%S'))).total_seconds()/60

	job_name=id.split("_")[2]


	job_inf_dict['id']=id
	job_inf_dict['job_name']=job_name
	job_inf_dict['mapsTotal']=mapsTotal 
	job_inf_dict['reducesTotal']=reducesTotal   
	job_inf_dict['duration_seconds']=elapsedTime
	job_inf_dict['total_time_task_seconds']=actualelapsedTime
	job_inf_dict['avgMapTime']=avgMapTime 
	job_inf_dict['avgReduceTime']=avgReduceTime
	
date=[]

def get_job(url,jobs):
	jobs=jobs
#	date=[]
	for job in jobs:
		job_inf_dict.clear()
#		print job
		current_date = lambda: int(round(time.time() * 1000))
		mili_date =  current_date()
		job_fin_time =  job['finishTime']
		job_id =  job['id']
		job_state =  job['state']
		job_state= job['state']
		#print job_state
		if job_state == "SUCCEEDED":

			date.append(job_fin_time)	
			job_inf = requests.get('http://%s/%s'%(url,job_id),headers = {'ACCEPT':'application/json'}).json().get('job')
			job_counter = requests.get('http://%s/%s/counters'%(url,job_id),headers = {'ACCEPT':'application/json'}).json().get('jobCounters')
	
			job_inf_process(job_inf,mili_date)
			job_counter_process(job_counter,job_id,mili_date)
			if len(job_inf_dict) == 12:
				print job_inf_dict['id'],"\t",job_inf_dict['job_name'],"\t",job_inf_dict['map_input_bytes'],"\t",job_inf_dict['SHUFFLE_BYTES'],"\t",job_inf_dict['reduce_output_bytes'],"\t",job_inf_dict['submitTime'],"\t",job_inf_dict['duration_seconds'],"\t",job_inf_dict['avgMapTime'],"\t",job_inf_dict['avgReduceTime'],"\t",job_inf_dict['total_time_task_seconds'],"\t",job_inf_dict['mapsTotal'],"\t",job_inf_dict['reducesTotal']
	



parser = OptionParser(usage="usage: ./parse-hadoop-jobhistory.py --url http://<HISTORY_SERVER>:<PORT>/ws/v1/history/mapreduce/jobs")
parser.add_option("-u", "--url",action="store_true",dest="xhtml_flag",default=False,help="mention the history server web address with port \n ex:-http://<HISTORY_SERVER>:<PORT>/ws/v1/history/mapreduce/jobs")
(options, args) = parser.parse_args()

if len(args) != 1:
	parser.error("wrong number of arguments")

if  args:
	start_jobs(args[0])
