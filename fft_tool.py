#!/usr/bin/python

""" Tool that reads a wav file, anaylzes it with FFT. 
	Creates 2 Tables:
		1) Meta Data on each sample
		2) Data of Each Sample. 
"""

import datetime

import MySQLdb
import sys
import wave
import os
import glob
import numpy
import logging
from scipy.io import wavfile as wav
from scipy.fftpack import fft
import matplotlib.pyplot as plt

#options: _LOG_LEVEL_STRINGS = ['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG']

log_level = 'DEBUG'

logging.basicConfig(level=log_level,
                    format='%(asctime)-3s %(levelname)-4s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p')

logging.debug('test = {0}'.format('foo'))
def fft_sample(phone_id):

	"""read all wav files """
	path = 'path'
	files = glob.glob(path)

	timestamp_dict = {}
	sample_collection = {}


	for file in files:

		rate, data = wav.read(file) 
		name = file.split(r'/')
		name = name[2].split(r'.')
		timestamp = name[0]
		

		n = data.size


		if data.size > 1:
			
			sample = numpy.fft.fft(data)
			freq = numpy.fft.fftfreq(n)
		

			timestamp_dict[timestamp] = (timestamp,sample,freq,phone_id)
			insert_to_fft_sample(timestamp_dict[timestamp])			

		   

def create_fft_sample_table():
	db = MySQLdb.connect(host="localhost",
                            user="user",
                            passwd="ps",
                            db="db")
    cur = db.cursor()
    cur.execute("DROP TABLE IF EXISTS fft_sample;")
    cur.execute("CREATE TABLE fft_sample (id MEDIUMINT NOT NULL AUTO_INCREMENT, phone_id VARCHAR(200) NOT NULL, start_time LONG NOT NULL, fft_state VARCHAR(200) NOT NULL,PRIMARY KEY (id));")
    db.commit()	
    db.close()


def create_fft_data_table():
	db = MySQLdb.connect(host="localhost",
                            user="user",
                            passwd="ps",
                            db="db")
    cur = db.cursor()
    cur.execute("DROP TABLE IF EXISTS fft_data;")
    cur.execute("CREATE TABLE fft_data (id VARCHAR(200), data VARCHAR(200));");
    db.close();

"""sample = (timestamp,sample,freq,phone_id)"""
def insert_to_fft_sample(sample):
	db = MySQLdb.connect(host="localhost",
                            user="user",
                            passwd="ps",
                            db="db")
	print(sample)
	time = str(sample[0])
	data = str(sample[1])
	freq = str(sample[2])
	phone = str(sample[3])
	cur = db.cursor()



	logging.debug("INSERT INTO fft_sample (phone_id,start_time, fft_state) VALUES ('{0}', {1}, {2});".format(phone, time, '0'))
    	cur.execute("INSERT INTO fft_sample (phone_id,start_time, fft_state) VALUES ('{0}', {1}, {2});".format(phone, time, '0'))
		
	cur.execute("SELECT LAST_INSERT_ID()")
	ID = cur.fetchall()
	insert_to_fft_data(ID[0],sample)
	
	for i in ID:
		print i
	db.commit()
	db.close()

def insert_to_fft_data(ID, sample):
	db = MySQLdb.connect(host="localhost",
                            user="user",
                            passwd="ps",
                            db="db")
    	cur = db.cursor()
    	time = str(sample[0])
	data = sample[1]
	freq = sample[2]
	phone = str(sample[3])

    cur.execute("SELECT phone_id FROM fft_sample ")
	for s in data:	
		row = ""
		for r in s:
			row += str(r)
	
		logging.debug("INSERT INTO fft_data (id, data)  VALUES ({0},'{1}');".format(ID[0],row))	
		cur.execute("INSERT INTO fft_data (id, data)  VALUES ({0},'{1}');".format(ID[0],row))
	db.commit()
 	db.close()

 

"""create a dictionary that contains for each timestamp a list of the adjacent samples collected in a span of 5 min
	for now only works for the same phone
"""
def create_sample_collection(timestamp_dict, sample_collection):
	for t in timestamp_dict:
		samples = []
		time = int(t)
		past_5_min = ((time / 100000) - 5) * 100000
		later_5_min = ((time / 100000) + 5) * 100000
		for i in timestamp_dict:
			i_time = int(i)
			if i_time <= later_5_min and i_time >= past_5_min:
				samples += [(i,timestamp_dict[i])]
		sample_collection[t] = (samples, t[3])


def mysql_test_connection():
	db = MySQLdb.connect(host="localhost",
			     	user="user",
				passwd="ps",
				db="db")		 
	cur = db.cursor()        
	cur.execute("SELECT * FROM fft_data LIMIT 10");
	for row in cur.fetchall():
		print row
	db.close();

# def mysql_connection()


def main():    
	 
	phone_id = "phone"
	mysql_test_connection();
   	create_fft_sample_table()
    create_fft_data_table()        
	fft_sample(phone_id)

main()   








