#!/usr/bin/env python
# -*- coding: utf-8 -*-
# python authoring.py


# Imports
from pymongo import MongoClient


# Main
if __name__ == "__main__":
	# MongoDB connection
	client 		= MongoClient('localhost', 27017)
	db 			= client.ccl
	mails 		= db.mails
	authors 	= db.authors

	# Clear all authors
	authors.remove({})

	# Iterate over mails
	# for mail in mails.find({'url':{'$regex':'\?2001\+'}}):
	for mail in mails.find():
		email = mail['email']
		# If email is not already an author, add it
		if authors.find({'email':email}).count() == 0:
			authors.insert({'email':email,'mails':[mail['_id']],'count':1,'startdate':mail['timestamp'],'enddate':mail['timestamp']})
		# Else increment the count and update the startdate and enddate fields if needed
		else:
			author = authors.find({'email':email})[0]
			startdate = mail['timestamp'] if mail['timestamp'] < author['startdate'] else author['startdate']
			enddate = mail['timestamp'] if mail['timestamp'] > author['enddate'] else author['enddate']
			authors.update({'_id':author['_id']},{'$set':{'startdate':startdate,'enddate':enddate},'$push':{'mails':mail['_id']}, '$inc':{'count':1}})