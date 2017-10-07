#!/usr/bin/python

""" Tool that reads a wav file, anaylzes it with FFT. 
	compares 5 past/future timesteps and runs a query to for the neighboring timesteps.
	build a classifier 
"""

import datetime

#import sqlite3
import matplotlib.pyplot as plt
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
def fft_sample(phone_id,path):

	db = db_connect()

	"""read all wav files """
	#path = './wit_audio/*.wav'
	files = glob.glob(path)

	timestamp_dict = {}
	sample_collection = {}


	for file in files:

		rate, data = wav.read(file) #need to figure out regex manipulation later. 
		name = file.split(r'/')
		name = name[2].split(r'.')
		timestamp = name[0]
		

		n = data[:,0].size

		
		if data.size > 1:
			
			sample_channel1 = numpy.fft.fft(data[:,0],norm="ortho")
			sample_channel2 = numpy.fft.fft(data[:,1],norm="ortho")
			print(sample_channel1[0])
			freq = numpy.fft.fftfreq(n,1.0/rate)
			magnitude1 = numpy.abs(sample_channel1)
			magnitude2 = numpy.abs(sample_channel2)
			# plot_sample(sample_channel1,freq)

			timestamp_dict[timestamp] = (timestamp,sample_channel1,sample_channel2,freq,phone_id,magnitude1,magnitude2,rate)
			insert_to_fft_sample(timestamp_dict[timestamp])
	


def create_fft_sample_table():
    db = db_connect()
    cur = db.cursor()
    cur.execute("DROP TABLE IF EXISTS fft_sample;")
    cur.execute("CREATE TABLE fft_sample (id MEDIUMINT NOT NULL AUTO_INCREMENT, phone_id VARCHAR(200) NOT NULL, start_time LONG NOT NULL, fft_state VARCHAR(200) NOT NULL,rate MEDIUMINT NOT NULL, PRIMARY KEY (id));")
    db.commit()	
    db.close() 

def create_fft_data_table():

    db = db_connect()
    cur = db.cursor()
    cur.execute("DROP TABLE IF EXISTS fft_data;")
    cur.execute("CREATE TABLE fft_data (id VARCHAR(200), data1 VARCHAR(200), magnitude1 DOUBLE,data2 VARCHAR(200), magnitude2 DOUBLE,freq DOUBLE);");
    db.close();


"""sample = (timestamp,sample,freq,phone_id,magnitude)"""
def insert_to_fft_sample(sample):

	db = db_connect()

	time = str(sample[0])
	phone = str(sample[4])
	rate = sample[7]
	cur = db.cursor()



	logging.debug("INSERT INTO fft_sample (phone_id,start_time, fft_state,rate) VALUES ('{0}', {1}, {2},{3});".format(phone, time, '0',rate))
	cur.execute("INSERT INTO fft_sample (phone_id,start_time, fft_state,rate) VALUES ('{0}', {1}, {2},{3});".format(phone, time, '0',rate))
		
	cur.execute("SELECT LAST_INSERT_ID()")
	ID = cur.fetchall()
	insert_to_fft_data(ID[0],sample)
	db.commit()
	db.close()



def insert_to_fft_data(ID, sample):

	db = db_connect()
	cur = db.cursor()
	time = str(sample[0])
	data1 = sample[1]
	data2 = sample[2]
	freq = sample[3]
	phone = sample[4]
	mag1 = sample[5]
	mag2 = sample[6]
	rate = sample[7]

	phone = str(sample[3]) 

	cur.execute("SELECT phone_id FROM fft_sample ")
	for i in range(len(data1)):
		row1 = ""
		row2 = ""
		mag1_row = mag1[i]
		mag2_row = mag2[i]
		row1 = data1[i]
		row2 = data2[i]
		#for j in range(len(data1)):
		#	row1 += str(data1[j])
		#	row2 += str(data2[j])
		freq_row =  str(freq[i])
		#id , data1, magnitude1 , data2, magnitude2, freq
		logging.debug("INSERT INTO fft_data (id, data1, magnitude1, data2, magnitude2, freq)  VALUES ({0},'{1}',{2},'{3}',{4},'{5}');".format(ID[0],row1,mag1_row, row2,mag2_row,freq_row))	
		cur.execute("INSERT INTO fft_data (id,data1, magnitude1,data2, magnitude2, freq)  VALUES ({0},'{1}',{2},'{3}',{4},'{5}');".format(ID[0],row1,mag1_row,row2,mag2_row,freq_row))	
	db.commit()
	db.close()	




"""create a dictionary that contains for each timestamp a list of the adjacent samples collected in a span of 5 min
	for now only works for the same phone
"""
def power_from_timestamp(phone_id,timestamp):
	db = MySQLdb.connect(host="localhost",
        		user="ben",
                        passwd="gridwatch",
                        db="TZ")
        cur = db.cursor() 
	past_5_min = ((timestamp / 100000) - 5) * 100000
	later_5_min = ((timestamp / 100000) + 5) * 100000
	logging.debug("SELECT mVoltage FROM phones WHERE mTime > {0} AND mTime < {1};".format(past_5_min,later_5_min))
        cur.execute("SELECT mVoltage FROM phones WHERE mTime > {0} AND mTime < {1};".format(past_5_min,later_5_min))
	power = cur.fetchall()
	if len(power) == 0:
		logging.debug("SELECT mTime FROM phones WHERE mTime > {0} LIMIT 1;".format(timestamp))
        	cur.execute("SELECT mTime FROM phones WHERE mTime > {0} LIMIT 1;".format(timestamp))
		T_future = cur.fetchall()
                logging.debug("SELECT mTime FROM phones WHERE mTime < {0} LIMIT 1;".format(timestamp))
                cur.execute("SELECT mTime FROM phones WHERE mTime < {0} LIMIT 1;".format(timestamp))
                T_past = cur.fetchall()
		db.commit()
		db.close()
		return (T_past,T_future)
	
	db.commit()
	db.close()
	return power


def plot_sample(sample,freq):
	t = numpy.arange(sample.size)


	magnitude = numpy.abs(sample)
	powerspec = numpy.abs(sample)**2
	print(magnitude)
	print(powerspec)
	print(freq)
	plt.plot(freq,magnitude)
	plt.xlim(0,500)
	
	plt.show()

def db_connect():
	db = MySQLdb.connect(host="localhost",
                    user="ben",
                    passwd="gridwatch",
                    db="TZ")
	return db






def mysql_test_connection():
	db = MySQLdb.connect(host="localhost",
			     	user="ben",
				passwd="gridwatch",
				db="TZ")		 
	cur = db.cursor()        
	cur.execute("SELECT * FROM fft_data LIMIT 10");
	for row in cur.fetchall():
		print row
	db.close();

def plot_sample(sample):
	t = numpy.arange(sample.size)
	print(sample[1])

	magnitude = numpy.abs(sample)
	powerspec = numpy.abs(sample)**2
	print(magnitude)
	print(powerspec)
	print(freq)
	plt.plot(freq,magnitude)
	plt.xlim(0,500)
	
	plt.show()

def main():    
	print(sys.argv) 
	phone_id = sys.argv[1]
	path = sys.argv[2]
	create = sys.argv[3]
	if create == 't':
   		create_fft_sample_table()
    		create_fft_data_table() 
	mysql_test_connection()       
	fft_sample(phone_id,path)
	#power_from_timestamp(22,1491565472482))
main()   








