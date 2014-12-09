#!/usr/bin/env python
# -*- coding: utf-8 -*-
# python emails.py


# Imports
from pymongo import MongoClient
from pprint import pprint


# Main
if __name__ == "__main__":
	# MongoDB connection
	client 		= MongoClient('localhost', 27017)
	db 			= client.ccl
	authors		= db.authors
	mails		= db.mails

	for author in authors.find().sort('count',-1):
		startdate = -1
		enddate = -1
		for mailId in author['mails']:
			if mails.find({'_id':mailId}).count() == 1:
				m = mails.find({'_id':mailId})[0]
				if (startdate == -1) or (startdate > m['timestamp']):
					startdate = m['timestamp']
				if (enddate == -1) or (enddate < m['timestamp']):
					enddate = m['timestamp']
			else:
				pprint('Mail id not found')
		authors.update({'_id':author['_id']},{'$set':{'startdate':startdate,'enddate':enddate}})