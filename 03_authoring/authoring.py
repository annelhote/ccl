#!/usr/bin/env python
# -*- coding: utf-8 -*-
# python authoring.py


# Imports
import datetime, io, pymongo, re
from pprint import pprint


# Log the message level and the message into a log file
# file				: file object	: the file into which write
# level				: string 		: the log level of the message (info, warning, alert ...)
# message 			: string 		: the message content to be logged
# return 			: void
def log(file, level, message):
	message = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S") + ' | ' + level + ' | ' + message + '\n'
	file.write(unicode(message))


# Log the message level and the message into a log file
# author01			: object		: first author to merge
# author02			: object		: second author to merge
# return			: void
def mergeAuthors(author01, author02):
	log(f, 'info', 'Merge authors : ' + str(author01['_id']) + ' and ' + str(author02['_id']))
	# Merge all the email addresses together and remove duplicates
	emails		= list(set(author01['emails'] + author02['emails']))
	# Merge all the mails together and remove duplicates
	mails 		= list(set(author01['mails'] + author02['mails']))
	count 		= len(mails)
	startdate 	= author01['startdate'] if author01['startdate'] < author02['startdate'] else author02['startdate']
	enddate 	= author01['enddate'] if author01['enddate'] > author02['enddate'] else author02['enddate']
	duration 	= enddate - startdate
	duration	= 1 if duration == 0 else duration
	# Insert the merged author into collection
	try:
		authorsCollection.insert({'emails':emails,'mails':mails,'count':count,'startdate':startdate,'enddate':enddate,'duration':duration,'merged':1})
	except:
		log(f, 'error', 'Try to merge ' + email01 + ' and ' + email02)
		log(f, 'error', 'Size of the list of emails to insert : ' + str(len(mails)))
	# Mark both authors as deleted
	authorsCollection.update({'_id':author01['_id']},{'$set':{'deleted':1}})
	authorsCollection.update({'_id':author01['_id']},{'$set':{'deleted':1}})


# Main
if __name__ == "__main__":
	# MongoDB connection
	client 				= pymongo.MongoClient('localhost', 27017)
	db 					= client.ccl
	mailsCollection 	= db.mails
	authorsCollection 	= db.authors

	# Clear all authors
	authorsCollection.remove({})

	# Open a log file
	f = io.open('authoring.log', 'w', encoding='utf-8')
	log(f, 'info', 'Start authoring')

	# Iterate over mails that have no @ in their email address
	for mail in mailsCollection.find({'email':{'$regex':'^[^@]*$'},'deleted':{'$exists':0}}).sort('emaillength',pymongo.DESCENDING):
		email = mail['email']
		if email != '':
			# If that email already exists
			if authorsCollection.find({'emails':{'$regex':email},'deleted':{'$exists':0}}).count() > 0:
				author 		= authorsCollection.find({'emails':{'$regex':email},'deleted':{'$exists':0}})[0]
				emails		= author['emails'] if email in author['emails'] else author['emails'] + [email]
				startdate	= mail['timestamp'] if mail['timestamp'] < author['startdate'] else author['startdate']
				enddate		= mail['timestamp'] if mail['timestamp'] > author['enddate'] else author['enddate']
				duration	= enddate - startdate
				duration	= 1 if duration == 0 else duration
				authorsCollection.update({'_id':author['_id']},{'$set':{'emails':emails,'startdate':startdate,'enddate':enddate,'duration':duration},'$push':{'mails':mail['_id']}, '$inc':{'count':1}})
			# Else add it
			else:
				authorsCollection.insert({'emails':[email],'mails':[mail['_id']],'count':1,'startdate':mail['timestamp'],'enddate':mail['timestamp'],'duration':1})

	# Iterate over mails that have an @ in their email address
	for mail in mailsCollection.find({'email':{'$regex':'@'},'deleted':{'$exists':0}}):
		email = mail['email']
		trunk = re.search(r'(.*)@', email).group(1)
		regex = re.compile('^' + trunk + '@')
		# If that email already exists
		if email != '' and authorsCollection.find({'emails':regex,'deleted':{'$exists':0}}).count() > 0:
			authors			= authorsCollection.find({'emails':regex,'deleted':{'$exists':0}}).sort('emaillength',pymongo.DESCENDING)
			emails 			= [email]
			mails 			= [mail['_id']]
			startdate 		= mail['timestamp']
			enddate 		= mail['timestamp']
			for author in authors:
				emails		= list(set(emails + author['emails']))
				mails 		= list(set(author['mails'] + [mail['_id']]))
				startdate	= min(startdate, author['startdate'])
				enddate		= max(enddate, author['enddate'])
				# Mark as deleted
				authorsCollection.update({'_id':author['_id']},{'$set':{'deleted':1}})
			count			= len(emails)
			duration		= enddate - startdate
			duration		= 1 if duration == 0 else duration
			authorsCollection.insert({'emails':emails,'count':count,'mails':mails,'startdate':startdate,'enddate':enddate,'duration':duration})
		# Else add it
		else:
			authorsCollection.insert({'emails':[email],'mails':[mail['_id']],'count':1,'startdate':mail['timestamp'],'enddate':mail['timestamp'],'duration':1})

	# Close the log file
	log(f, 'info', 'End authoring')
	f.close()