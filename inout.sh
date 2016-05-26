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


def getVesselsIn():
	dbFrontend = MySQLdb.connect(MYSQL_IP, MYSQL_USER, MYSQL_PASSWORD, MYSQL_NAME)

	cursor = dbFrontend.cursor()
	query = """SELECT 
		DEVICE_ID
		FROM TRACKING_1
		WHERE 
		round(POS_LATITUDE_DEGREE,5) + round(POS_LATITUDE_MIN/60,5) < lat_max AND
		round(POS_LATITUDE_DEGREE,5) + round(POS_LATITUDE_MIN/60,5) > lat_min AND
		round(POS_LONGITUDE_DEGREE,5) + round(POS_LONGITUDE_MIN/60,5) < lon_max AND
		round(POS_LONGITUDE_DEGREE,5) + round(POS_LONGITUDE_MIN/60,5) > lon_min
		"""

	query = query.replace('lat_max', MAX_LAT)
	query = query.replace('lat_min', MIN_LAT)
	query = query.replace('lon_max', MAX_LON)
	query = query.replace('lon_min', MIN_LON)

	#print query
	cursor.execute(query)
	result = cursor.fetchall()
	
	try:
		return result
	except Exception, error:
		logger.error('Error getting data from database: %s.', error )
		
	cursor.close
	dbFrontend.close

def putVesselsIn(vesselList):
	dbFrontend = MySQLdb.connect(MYSQL_IP, MYSQL_USER, MYSQL_PASSWORD, MYSQL_NAME)

	cursor = dbFrontend.cursor()
	query = """UPDATE 
		VEHICLE
		SET VEHICLE.INOUT=1
		WHERE 
		DEVICE_ID in (vesselList)
		"""

	query = query.replace('vesselList', vesselList)

	#print query
	cursor.execute(query)
	result = cursor.fetchall()
	
	try:
		return result
	except Exception, error:
		logger.error('Error getting data from database: %s.', error )
		
	cursor.close
	dbFrontend.close

def getVesselsOut():
	dbFrontend = MySQLdb.connect(MYSQL_IP, MYSQL_USER, MYSQL_PASSWORD, MYSQL_NAME)

	cursor = dbFrontend.cursor()
	query = """SELECT 
		DEVICE_ID
		FROM TRACKING_1
		WHERE 
		round(POS_LATITUDE_DEGREE,5) + round(POS_LATITUDE_MIN/60,5) > lat_max OR
		round(POS_LATITUDE_DEGREE,5) + round(POS_LATITUDE_MIN/60,5) < lat_min OR
		round(POS_LONGITUDE_DEGREE,5) + round(POS_LONGITUDE_MIN/60,5) > lon_max OR
		round(POS_LONGITUDE_DEGREE,5) + round(POS_LONGITUDE_MIN/60,5) < lon_min
		"""

	query = query.replace('lat_max', MAX_LAT)
	query = query.replace('lat_min', MIN_LAT)
	query = query.replace('lon_max', MAX_LON)
	query = query.replace('lon_min', MIN_LON)

	#print query
	cursor.execute(query)
	result = cursor.fetchall()
	
	try:
		return result
	except Exception, error:
		logger.error('Error getting data from database: %s.', error )
		
	cursor.close
	dbFrontend.close

def putVesselsOut(vesselList):
	dbFrontend = MySQLdb.connect(MYSQL_IP, MYSQL_USER, MYSQL_PASSWORD, MYSQL_NAME)

	cursor = dbFrontend.cursor()
	query = """UPDATE 
		VEHICLE
		SET VEHICLE.INOUT=0
		WHERE 
		DEVICE_ID in (vesselList)
		"""

	query = query.replace('vesselList', vesselList)

	#print query
	cursor.execute(query)
	result = cursor.fetchall()
	
	try:
		return result
	except Exception, error:
		logger.error('Error getting data from database: %s.', error )
		
	cursor.close
	dbFrontend.close

vIn = getVesselsIn()
vesselsIn = ''
numVesselsIn = 0
for ele in vIn:	
	if (numVesselsIn==0):
		vesselsIn = str(ele[0])
	else:
		vesselsIn = vesselsIn + ',' + str(ele[0])
	numVesselsIn += 1
putVesselsIn(vesselsIn)

vOut = getVesselsOut()
vesselsOut = ''
numVesselsOut = 0
for eleOut in vOut:	
	if (numVesselsOut==0):
		vesselsOut = str(eleOut[0])
	else:
		vesselsOut = vesselsOut + ',' + str(eleOut[0])
	numVesselsOut += 1
putVesselsOut(vesselsOut)

print "Vessels in : " + str(numVesselsIn)
print "Vessels out: " + str(numVesselsOut)
