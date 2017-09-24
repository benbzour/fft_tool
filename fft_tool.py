""" Tool that reads a wav file, anaylzes it with FFT. 
	compares 5 past/future timesteps and runs a query to for the neighboring timesteps.
	build a classifier 
"""

import datetime

#import sqlite3

import MySQLdb
import sys
import wave
import os
import glob
import numpy
from scipy.io import wavfile as wav
from scipy.fftpack import fft
import matplotlib.pyplot as plt



def fft_sample(phone_id):

	"""read all wav files """
	path = 'path'
	files = glob.glob(path)

	timestamp_dict = {}
	sample_collection = {}


	for file in files:

		rate, data = wav.read(file) #need to figure out regex manipulation later. 
		name = file.split(r'/')
		name = name[2].split(r'.')
		timestamp = name[0]
		

		n = data.size


		if data.size > 1:
			# sample = numpy.fft.fft2(data) #not sure which one is better yet...
			sample = numpy.fft.fft(data)
			freq = numpy.fft.fftfreq(n)
		

			timestamp_dict[timestamp] = (timestamp,sample,freq,phone_id)
			insert_to_fft_sample(timestamp_dict[timestamp])			
# sample query = sample_query(cdb_conn,(timestamp,sample,freq))
			# create_sample_table(db_conn, sample_query,timestamp_dict[timestamp])
		#	create_sample_collection(timestamp_dict,sample_collection)
#	print(sample_collection)


	


		###different way to analyze using lib wave###
		# w = wave.open(file, 'r')
		# channels = w.getnchannels()
		# sample_width = w.getsampwidth()
		# params = w.getparams()
		# for i in range(w.getnchannels()):
		    # frame = w.readframes(i)
		   

def create_fft_sample_table():
    db = MySQLdb.connect(host="localhost",
                            user="user",
                            passwd="ps",
                            db="db")
    cur = db.cursor()
    cur.execute("DROP TABLE fft_sample;")
    cur.execute("CREATE TABLE fft_sample (id MEDIUMINT NOT NULL AUTO_INCREMENT, phone_id VARCHAR(200) NOT NULL, start_time VARCHAR(200) NOT NULL, fft_state VARCHAR(200) NOT NULL,PRIMARY KEY (id));")
    db.close()


def create_fft_data_table():
    db = MySQLdb.connect(host="localhost",
                            user="user",
                            passwd="ps",
                            db="db")
    cur = db.cursor()
    cur.execute("DROP TABLE fft_data;")
    cur.execute("CREATE TABLE fft_data (id VARCHAR(200), data VARCHAR(200));");
    db.close();

"""sample = (timestamp,sample,freq,phone_id)"""
def insert_to_fft_sample(sample):
    db = MySQLdb.connect(host="localhost",
                            user="user",
                            passwd="ps",
                            db="db")

	time = str(sample[0])
	data = str(sample[1])
	freq = str(sample[2])
	phone = str(sample[3])
	cur = db.cursor()

        #cur.execute("SELECT * FROM fft_sample LIMIT 10");


    	cur.execute("INSERT INTO fft_sample (phone_id,start_time, fft_state) VALUES (%s, %s, %s)",(phone, time, '0'))
	cur.execute("SELECT id FROM fft_sample WHERE start_time = %s AND phone_id = %s;",(time,phone))
	cur.execute("SELECT * FROM fft_sample LIMIT 10");
	ID = cur.fetchall()
	insert_to_fft_data(ID[0],sample)
	#for i in ID:
	#	print i
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
		cur.execute("INSERT INTO fft_data (id, data)  VALUES (%s,%s);",(ID[0],row))

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



def main():    
	 
	phone_id = sys.argv[1]
	#mysql_test_connection();
   	#create_fft_sample_table()
    #create_fft_data_table()        
	#fft_sample(phone_id)

main()   








