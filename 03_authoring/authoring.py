#!/usr/bin/env python
# -*- coding: utf-8 -*-
# python authoring.py


# Imports
import io, datetime, re
from pprint import pprint
from pymongo import MongoClient


# Log the message level and the message into a log file
# file				: file object	: the file into which write
# level				: string 		: the log level of the message (info, warning, alert ...)
# message 			: string 		: the message content to be logged
# return 			: void
def log(file, level, message):
	message = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S") + ' | ' + level + ' | ' + message + '\n'
	file.write(unicode(message))


# Log the message level and the message into a log file
# email01			: string		: email of the first author to merge
# email02			: string		: email of the second author to merge
# return			: void
def mergeAuthors(email01, email02):
	log(f, 'info', 'Merge email addresses : ' + email01 + ' and ' + email02)
	if authorsCollection.find({'emails':email01,'deleted':{'$exists':False}}).count() == 1 and authorsCollection.find({'emails':email02,'deleted':{'$exists':False}}).count() == 1:
		author01 = authorsCollection.find({'emails':email,'deleted':{'$exists':False}})[0]
		author02 = authorsCollection.find({'emails':email,'deleted':{'$exists':False}})[0]
		emails = author01['emails'] + author02['emails']
		mails = author01['mails'] + author02['mails']
		count = author01['count'] + author02['count']
		startdate = author01['startdate'] if author01['startdate'] < author02['startdate'] else author02['startdate']
		enddate = author01['enddate'] if author01['enddate'] > author02['enddate'] else author02['enddate']
		# Insert the merged author into collection
		try:
			authorsCollection.insert({'emails':emails,'mails':mails,'count':count,'startdate':startdate,'enddate':enddate,'merged':1})
		except:
			log(f, 'error', 'Size of the list of emails to insert : ' + str(len(mails)))
		# Mark both authors as deleted
		authorsCollection.update({'_id':author01['_id']},{'$set':{'deleted':1}})
		authorsCollection.update({'_id':author01['_id']},{'$set':{'deleted':1}})
	else:
		log(f, 'warning', 'Duplicate email address into authors collection (either ' + email01 + ' or ' + email02 + ')')


# Main
if __name__ == "__main__":
	# MongoDB connection
	client 				= MongoClient('localhost', 27017)
	db 					= client.ccl
	mailsCollection 	= db.mails
	authorsCollection 	= db.authors

	# Clear all authors
	authorsCollection.remove({})

	# Open a log file
	f = io.open('error.log', 'w', encoding='utf-8')
	log(f, 'info', 'Start authoring')

	# Iterate over mails
	# for mail in mails.find({'url':{'$regex':'\?2001\+'}}):
	for mail in mailsCollection.find():
		email = mail['email']
		# If email is not already an author, add it
		if authorsCollection.find({'emails':email,'deleted':{'$exists':False}}).count() == 0:
			authorsCollection.insert({'emails':[email],'mails':[mail['_id']],'count':1,'startdate':mail['timestamp'],'enddate':mail['timestamp']})
		# Else increment the count and update the startdate and enddate fields if needed
		else:
			author = authorsCollection.find({'emails':email,'deleted':{'$exists':False}})[0]
			startdate = mail['timestamp'] if mail['timestamp'] < author['startdate'] else author['startdate']
			enddate = mail['timestamp'] if mail['timestamp'] > author['enddate'] else author['enddate']
			authorsCollection.update({'_id':author['_id']},{'$set':{'startdate':startdate,'enddate':enddate},'$push':{'mails':mail['_id']}, '$inc':{'count':1}})

	for author in authorsCollection.find():
		for email in author['emails']:
			# Get first part of the author email before the @
			if re.search(r'(.*)@', email):
				trunk = re.search(r'(.*)@', email).group(1)
				# Find similar email addresses
				similaremails = authorsCollection.find({'emails':{'$regex':'^' + trunk + '@'}})
				for similaremail in similaremails:
					if len(similaremail['emails']) > 1:
						log(f,'error','The size of this author\'s emails is bigger than 1 : ' + str(author['_id']))
					else:
						# Merge them together
						mergeAuthors(email,similaremail['emails'][0])
			else:
				log(f,'info','This email address has no @ : ' + email)

	# Close the log file
	log(f, 'info', 'End authoring')
	f.close()