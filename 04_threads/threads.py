#!/usr/bin/python
# python threading.py


# Imports
import re, string, io, datetime
from pymongo import MongoClient
from pprint import pprint
from difflib import SequenceMatcher


# Log the message level and the message into a log file
# file		: file object	: the file into which write
# level		: string 		: the log level of the message (info, warning, alert ...)
# message 	: string 		: the message content to be logged
# return 	: void
def log(file, level, message):
	message = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S") + ' | ' + level + ' | ' + message + '\n'
	file.write(unicode(message))


# Normalize a string ie. transform it into lowercase, remove prepend 're:' and 'ccl:' and filter non alphanumeric character
# Recursive function
# title		: string 		: the string to normalize
# return 	: string 		: the normalized title
def normalize(title):
	title	= string.lower(title)
	search	= re.search('^ *(re|ccl) *: *(.*)', title, flags=re.IGNORECASE)
	if search:
		return normalize(search.group(2))
	else:
		return re.sub(r'[^\w\d]', '', title)


# Attach a mail into a thread given its thread id
# Update thread parameters according to mail parameter
# mail		: object 		: email object to attachto a thread
# threads	: iterator 		: iterator on a collection of threads
# threadId 	: int 			: unique id to identify the thread into the threads collection
# return 	: bool			: return true if the mail has really been added to the mails field of that thread, false otherwise, ie. if this mail id was already in that thread
def addMailToThreadById(mail, threads, threadId):
	# Retrieve list of mails ids already attached to this thread
	currentThread 	= threads.find({'_id':threadId})[0]
	mails 			= currentThread['mails']
	# If the current mail id it not alreaddy attached to this thread
	if not mail['_id'] in mails:
		# TODO : group all the updates into a single db request ???
		# Attach the current mail id to the pointed thread
		threads.update({'_id':threadId},{'$push':{'mails':mail['_id']},'$inc':{'count':1}})
		# Update other fields of this thread
		startDate 	= currentThread['startdate']
		endDate 	= currentThread['enddate']
		if startDate == -1 or mail['timestamp'] < startDate :
			startDate 	= mail['timestamp']
			duration 	= endDate - startDate
			threads.update({'_id':threadId},{'$set':{'startdate':startDate,'duration':duration}})
		if endDate == -1 or mail['timestamp'] > endDate :
			endDate 	= mail['timestamp']
			duration	= endDate - startDate
			threads.update({'_id':threadId},{'$set':{'enddate':mail['timestamp'],'duration':duration}})


# Attach a mail into the threads collection
# Try to rattach the mail into an existing one if it is pertinent, otherwise create a new thread
# mail		: object 		: email object to attachto a thread
# threads	: iterator 		: iterator on a collection of threads
# threashold: int 			: similarity threshold of acceptance between a thread title and a mail title
# return 	: object		: Id of the thread into which the mail have been added
def addMailToThreads(mail, threads, threshold, threadDuration):
	completeTitle 	= mail['subject']
	title 			= normalize(completeTitle)
	for thread in threads.find({}):
		if (thread['title'] != '' and SequenceMatcher(None, title, thread['title']).ratio() >= threshold) and (abs(mail['timestamp'] - thread['startdate']) <= threadDuration):
			threadId = thread['_id']
			addMailToThreadById(mail, threads, threadId)
			return threadId
	threadId = threads.insert({'title':title,'completeTitle':completeTitle,'mails':[],'count':0,'startdate':-1,'enddate':-1,'duration':-1})
	addMailToThreadById(mail, threads, threadId)
	return threadId


# Attach a list of mails into the same thread in the threads collection
# mails		: array 		: array of mail object
# threads	: iterator 		: iterator on a collection of threads
# threashold: int 			: similarity threshold of acceptance between a thread title and a mail title
# return 	: void
def addMailsToThreads(mails, threads, threshold, threadDuration):
	threadId = addMailToThreads(mails.pop(0), threads, threshold, threadDuration)
	for mail in mails:
		addMailToThreadById(mail, threads, threadId)


# Main
if __name__ == "__main__":
	# MongoDB connection
	client 		= MongoClient('localhost', 27017)
	db 			= client.ccl
	mails 		= db.mails
	threads 	= db.threads
	count		= 0
	threshold	= 0.9
	# 3 months
	threadDuration	= 3 * 30 * 24 * 60 * 60

	# Clear all threads
	threads.remove({})

	# Open a log file
	f = io.open('error.log', 'w', encoding='utf-8')
	log(f, 'info', 'Start threading')

	for mail in mails.find(timeout=False):
		# 1. Check if a x-reference field is not empty
		xreference = mail['X-Reference']
		if xreference != '':
			# Get the email referenced with that 'X-Message-Id'
			if mails.find({'X-Message-Id':xreference}).count() == 1:
				origin = mails.find({'X-Message-Id':xreference})[0]
				addMailsToThreads([origin, mail], threads, threshold, threadDuration)
				continue
			else:
				count += 1
				log(f, 'warning', 'No referenced message with id : ' + xreference)
		# 2. Add this mail to a thread
		addMailsToThreads([mail], threads, threshold, threadDuration)

	log(f, 'info', 'Number of mails processed : ' + str(mails.find().count()))
	log(f, 'warning', 'Number of missing reference : ' + str(count))
	
	# Close the log file
	f.close()