#!/usr/bin/python
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

	for mail in mails.find():
		email = mail['email']
		if authors.find({'email':email}).count() == 0:
			authors.insert({'email':email,'mails':[mail['_id']],'count':1})
		else:
			authors.update({'email':email},{'$push':{'mails':mail['_id']},'$inc':{'count':1}})