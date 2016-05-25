#!/usr/bin/env python
#-*- coding: UTF-8 -*-

# autor: Carlos Rueda
# date: 2016-05-25
# version: 1.1

##################################################################################
# version 1.0 release notes: extract data from MySQL and generate json
# Initial version
# Requisites: library python-mysqldb. To install: "apt-get install python-mysqldb"
##################################################################################


import MySQLdb
import logging, logging.handlers
import os
import json
import sys
import datetime
import calendar
import time

#### VARIABLES #########################################################
from configobj import ConfigObj
config = ConfigObj('./inout.properties')

INTERNAL_LOG_FILE = config['directory_logs'] + "/inout.log"
LOG_FOR_ROTATE = 10

MYSQL_IP = config['mysql_host']
MYSQL_PORT = config['mysql_port']
MYSQL_USER = config['mysql_user']
MYSQL_NAME = config['mysql_db_name']
MYSQL_PASSWORD = config['mysql_passwd']

MAX_LAT = config['max_lat']
MIN_LAT = config['min_lat']
MAX_LON = config['max_lon']
MIN_LON = config['min_lon']

########################################################################
# definimos los logs internos que usaremos para comprobar errores
try:
	logger = logging.getLogger('inout')
	loggerHandler = logging.handlers.TimedRotatingFileHandler(INTERNAL_LOG_FILE , 'midnight', 1, backupCount=10)
	formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
	loggerHandler.setFormatter(formatter)
	logger.addHandler(loggerHandler)
	logger.setLevel(logging.DEBUG)
except:
	print '------------------------------------------------------------------'
	print '[ERROR] Error writing log at %s' % INTERNAL_LOG_FILE
	print '[ERROR] Please verify path folder exits and write permissions'
	print '------------------------------------------------------------------'
	exit()
########################################################################

########################################################################


def getIn():
	dbKyros4 = MySQLdb.connect(MYSQL_IP, MYSQL_USER, MYSQL_PASSWORD, MYSQL_NAME)
	try:
		dbKyros4 = MySQLdb.connect(MYSQL_IP, MYSQL_USER, MYSQL_PASSWORD, MYSQL_NAME)
	except:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s', MYSQL_IP, MYSQL_USER, MYSQL_PASSWORD, MYSQL_NAME)

	cursor = dbKyros4.cursor()
	cursor.execute("""SELECT 
		DEVICE_ID
		round(POS_LATITUDE_DEGREE,5) + round(POS_LATITUDE_MIN/60,5) as LAT, 
		round(POS_LONGITUDE_DEGREE,5) + round(POS_LONGITUDE_MIN/60,5) as LON, 
		round(GPS_SPEED,1) as speed,
		round(HEADING,1) as heading,
		POS_DATE as DATE 
		FROM TRACKING_1
		WHERE 
		round(POS_LATITUDE_DEGREE,5) + round(POS_LATITUDE_MIN/60,5) < lat_max AND
		round(POS_LATITUDE_DEGREE,5) + round(POS_LATITUDE_MIN/60,5) > lat_min AND
		round(POS_LONGITUDE_DEGREE,5) + round(POS_LONGITUDE_MIN/60,5) < lon_max AND
		round(POS_LONGITUDE_DEGREE,5) + round(POS_LONGITUDE_MIN/60,5) > lon_min
		""")
	result = cursor.fetchall()
	
	try:
		return result
	except Exception, error:
		logger.error('Error getting data from database: %s.', error )
		
	cursor.close
	dbFrontend.close

array_list = []
trackingInfo = getTracking()
ntrackings = 0

for tracking in trackingInfo:
	position = {"geometry": {"type": "Point", "coordinates": [ tracking[1] , tracking[0] ]}, "type": "Feature", "properties":{"speed": tracking[2], "heading": tracking[3]}}
	array_list.append(position)
	ntrackings+=1

print ntrackings

with open('./tracking.json', 'w') as outfile:
	json.dump(array_list, outfile)