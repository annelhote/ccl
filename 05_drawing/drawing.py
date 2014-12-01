#!/usr/bin/python\+
# python drawing.py

# Imports
import json, io, time, calendar
from pymongo import MongoClient
from pprint import pprint


def extractData(year, month, day):
	if year and month and day:
		regex 					= '\?' + str(year) + '\+' + str(month).zfill(2) + '\+' + str(day).zfill(2) + '\+'
		startDate 				= int(time.mktime((int(year), int(month), int(day), 0, 0, 0, 0, 0, 0)))
		endDate 				= int(time.mktime((int(year), int(month), int(day) + 1, 0, 0, 0, 0, 0, 0))) - 1
	elif year and month:
		regex 					= '\?' + str(year) + '\+' + str(month).zfill(2) + '\+'
		startDate 				= int(time.mktime((int(year), int(month), 1, 0, 0, 0, 0, 0, 0)))
		endDate 				= int(time.mktime((int(year), int(month) + 1, 1, 0, 0, 0, 0, 0, 0))) - 1
	elif year:
		regex 					= '\?' + str(year) + '\+'
		startDate 				= int(time.mktime((int(year), 1, 1, 0, 0, 0, 0, 0, 0)))
		endDate 				= int(time.mktime((int(year) + 1, 1, 1, 0, 0, 0, 0, 0, 0))) - 1
	else :
		regex 					= ''
		startDate 				= int(time.mktime((1991, 1, 1, 0, 0, 0, 0, 0, 0)))
		endDate 				= int(time.mktime((2015, 1, 1, 0, 0, 0, 0, 0, 0))) - 1
	# Count all mails
	dataMails 					= mails.find({'url':{'$regex':regex}}).count()
	# Count all empty email addresses
	dataNoemailaddress 			= mails.find({'url':{'$regex':regex}, 'email':''}).count()
	# Calculate percentage of empty email addresses
	dataNoemailaddressPercent	= "{0:.2f}".format(dataNoemailaddress / float(dataMails) * 100) if dataMails != 0 else 0
	# Count all not empty 'X-Reference'
	dataXreference 				= mails.find({'url':{'$regex':regex}, 'X-Reference':{'$ne':''}}).count()
	# Calculate percentage of not empty 'X-Reference'
	dataXreferencePercent 		= "{0:.2f}".format(dataXreference / float(dataMails) * 100) if dataMails != 0 else 0
	# Count all threads
	dataThreads 				= threads.find({'startdate':{'$gt':startDate},'enddate':{'$lt':endDate}}).count()
	dataThreads 				= 0
	return (dataMails, dataNoemailaddress, dataNoemailaddressPercent, dataXreference, dataXreferencePercent, dataThreads)


def interateOverTime(mails):
	y_array = []
	(y_emails, y_noemailaddress, y_noemailaddresspercent, y_xreference, y_xreferencepercent, y_threads) = extractData(None, None, None)
	y_array.append({'year':'total','mails':y_emails,'noemailaddress':y_noemailaddress,'xreference':y_xreference,'threads':y_threads})
	# Iterate over years
	for y_year in range(1991, 2015):
		(y_emails, y_noemailaddress, y_noemailaddresspercent, y_xreference, y_xreferencepercent, y_threads) = extractData(y_year, None, None)
		# Iterate over months
		m_array = []
		for m_month in range(1, 13):
			(m_emails, m_noemailaddress, m_noemailaddresspercent, m_xreference, m_xreferencepercent, m_threads) = extractData(y_year, m_month, None)
			# Iterate over days
			d_array = []
			for d_day in range(1, calendar.mdays[m_month] + 1):
				(d_emails, d_noemailaddress, d_noemailaddresspercent, d_xreference, d_xreferencepercent, d_threads) = extractData(y_year, m_month, d_day)
				d_array.append({'day':d_day,'mails':d_emails,'noemailaddress':d_noemailaddress,'noemailaddresspercent':d_noemailaddresspercent,'xreference':d_xreference,'xreferencepercent':d_xreferencepercent,'threads':d_threads})
			m_array.append({'month':m_month,'mails':m_emails,'noemailaddress':m_noemailaddress,'noemailaddresspercent':m_noemailaddresspercent,'xreference':m_xreference,'xreferencepercent':m_xreferencepercent,'threads':m_threads,'days':d_array})
		y_array.append({'year':y_year,'mails':y_emails,'noemailaddress':y_noemailaddress,'noemailaddresspercent':y_noemailaddresspercent,'xreference':y_xreference,'xreferencepercent':y_xreferencepercent,'threads':y_threads,'months':m_array})
	return y_array


# Main
if __name__ == "__main__":
	# MongoDB connection
	client 		= MongoClient('localhost', 27017)
	db 			= client.ccl
	mails 		= db.mails
	threads 	= db.threads

	# Clean and update mails
	data = interateOverTime(mails)
	# Write results into a json file
	f = io.open('data.json', 'w', encoding='utf-8')
	f.write(unicode(json.dumps(data, separators=(',',':'), indent=4)))
	f.close()