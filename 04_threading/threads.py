#!/usr/bin/env python
# -*- coding: utf-8 -*-
# python threads.py


# Imports
import datetime, io, re, string
from pymongo import MongoClient
from pprint import pprint
from difflib import SequenceMatcher


# Log the message level and the message into a log file
# file						: file object	: the file into which write
# level						: string 		: the log level of the message (info, warning, alert ...)
# message 					: string 		: the message content to be logged
# return 					: void
def log(file, level, message):
	message = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S") + ' | ' + level + ' | ' + message + '\n'
	file.write(unicode(message))


# Normalize a string ie. transform it into lowercase, remove prepend 're:' and 'ccl:' and filter non alphanumeric character
# Recursive function
# title						: string 		: the string to normalize
# return 					: string 		: the normalized title
def normalize(title):
	title	= string.lower(title)
	search	= re.search('^ *(re|ccl) *: *(.*)', title, flags=re.IGNORECASE)
	if search:
		return normalize(search.group(2))
	else:
		return re.sub(r'[^\w\d]', '', title)


# Attach a mail into a thread given its thread id
# Update thread parameters according to mail parameter
# mail						: object 		: email object to attachto a thread
# threadId 					: int 			: unique id to identify the thread into the threads collection
# return 					: bool			: return true if the mail has really been added to the mails field of that thread, false otherwise, ie. if this mail id was already in that thread
def addMailToThreadById(mail, threadId):
	# Retrieve list of mails ids already attached to this thread
	currentThread 	= threadsCollection.find({'_id':threadId})[0]
	mails 			= currentThread['mails']
	# If the current mail id it not alreaddy attached to this thread
	if not mail['_id'] in mails:
		# Attach the current mail id to the pointed thread and update other fields of this thread
		# Take the longest title
		completeTitle	= currentThread['completeTitle'] if len(currentThread['completeTitle']) > len(normalize(mail['title'])) else normalize(mail['title'])
		title 			= normalize(completeTitle)
		startdate 		= mail['timestamp'] if (currentThread['startdate'] == -1 or mail['timestamp'] < currentThread['startdate']) else currentThread['startdate']
		enddate 		= mail['timestamp'] if (currentThread['enddate'] == -1 or mail['timestamp'] > currentThread['enddate']) else currentThread['enddate']
		duration 		= enddate - startdate
		duration 		= 1 if duration == 0 else duration
		threadsCollection.update({'_id':threadId},{'$set':{'completeTitle':completeTitle,'title':title,'startdate':startdate,'enddate':enddate,'duration':duration},'$push':{'mails':mail['_id']},'$inc':{'count':1}})


# Attach a mail into the threads collection
# Try to rattach the mail into an existing one if it is pertinent, otherwise create a new thread
# mail						: object 		: email object to attachto a thread
# threashold				: int 			: similarity threshold of acceptance between a thread title and a mail title
# threadDuration			: int 			: validity duration of a thread
# minimalLengthSubject 		: int 			: minimal number of characters of the normalized title to check if it is contains into a thread title
# return 					: object		: Id of the thread into which the mail have been added
def addMailToThreads(mail, threshold, threadDuration, minimalLengthSubject):
	completeTitle 	= mail['subject']
	title 			= normalize(completeTitle)
	sup				= mail['timestamp'] - threadDuration
	inf				= mail['timestamp'] + threadDuration
	for thread in threadsCollection.find({'startdate':{'$gt':sup},'enddate':{'$lt':inf}}):
		if ((thread['title'] != '') and (SequenceMatcher(None, title, thread['title']).ratio() >= threshold) and (abs(mail['timestamp'] - thread['startdate']) <= threadDuration)) or ((len(title) >= minimalLengthSubject) and (len(thread['title']) >= minimalLengthSubject) and (title in thread['title'])):
			threadId = thread['_id']
			addMailToThreadById(mail, threadId)
			return threadId
	threadId = threadsCollection.insert({'title':title,'completeTitle':completeTitle,'mails':[],'count':0,'startdate':-1,'enddate':-1,'duration':-1})
	addMailToThreadById(mail, threadId)
	return threadId


# Attach a list of mails into the same thread in the threads collection
# mails						: array 		: array of mail object
# threashold				: int 			: similarity threshold of acceptance between a thread title and a mail title
# threadDuration			: int 			: validity duration of a thread
# minimalLengthSubject 		: int 			: minimal number of characters of the normalized title to check if it is contains into a thread title
# return 					: void
def addMailsToThreads(mails, threshold, threadDuration, minimalLengthSubject):
	threadId = addMailToThreads(mails.pop(0), threshold, threadDuration, minimalLengthSubject)
	for mail in mails:
		addMailToThreadById(mail, threadId)


# Merge threads together
# fromThreadTitle			: string		: normalized title of the thread to merge
# toThreadTitle				: string		: normalized title of the thread to merge into
# return					: void
def mergeThreads(fromThreadTitle, toThreadTitle):
	fromThread 			= threadsCollection.find({'title':fromThreadTitle})
	toThread 			= threadsCollection.find({'title':toThreadTitle})
	if fromThread.count() == 1 and toThread.count() == 1:
		fromThread 		= fromThread[0]
		toThread 		= toThread[0]
		startdate 		= min(fromThread['startdate'], toThread['startdate'])
		enddate 		= max(fromThread['enddate'], toThread['enddate'])
		duration 		= enddate - startdate
		duration 		= 1 if duration == 0 else duration
		# Take the longest title
		completeTitle 	= fromThread['completeTitle'] if len(fromThread['completeTitle']) > len(toThread['completeTitle']) else toThread['completeTitle']
		title 			= normalize(completeTitle)
		mails 			= list(set(fromThread['mails'] + toThread['mails']))
		count 			= len(mails)
		threadsCollection.update({'_id':fromThread['_id']},{'$set':{'deleted':1}})
		threadsCollection.update({'_id':toThread['_id']},{'$set':{'deleted':1}})
		threadsCollection.insert({'title':title,'completeTitle':completeTitle,'mails':mails,'count':count,'startdate':startdate,'enddate':enddate,'duration':duration,'merged':1})
	else:
		log(f, 'error', 'None or multiple threads have that title !')


# Main
if __name__ == "__main__":
	# MongoDB connection
	client 					= MongoClient('localhost', 27017)
	db 						= client.ccl
	mailsCollection			= db.mails
	threadsCollection		= db.threads
	count					= 0
	threshold				= 0.9
	minimalLengthSubject 	= 10
	# 3 months
	threadDuration			= 3 * 30 * 24 * 60 * 60
	blackListedTitles		= ['bitnetmailfollows', 'thisisatest', 'help', 'softwareforreview']

	# Clear all threads
	threadsCollection.remove({})

	# Open a log file
	f = io.open('threads.log', 'w', encoding='utf-8')
	log(f, 'info', 'Start threading')

	for mail in mailsCollection.find(timeout=False):
		# 1. Check if a xreference field is not empty
		xreference = mail['xreference']
		if xreference != '':
			# Get the email referenced with that 'xmessageid'
			if mailsCollection.find({'xmessageid':xreference}).count() > 0:
				origin = mailsCollection.find({'xmessageid':xreference})[0]
				addMailsToThreads([origin, mail], threshold, threadDuration, minimalLengthSubject)
			else:
				count += 1
				log(f, 'warning', 'No referenced message with id : ' + xreference)
		# 2. Add this mail to a thread
		addMailsToThreads([mail], threshold, threadDuration, minimalLengthSubject)

	log(f, 'info', 'Number of mails processed : ' + str(mailsCollection.find().count()))
	log(f, 'warning', 'Number of missing reference : ' + str(count))

	# Mark blacklisted threads as deleted
	threadsCollection.update({'title':{'$in':blackListedTitles}},{'$set':{'deleted':1}})

	# Merge specified threads
	mergeThreads('objectorientedmeansforcomputationalchemistry', 'objectorientedmeansforcomputational')
	# 1993_52.ods
	mergeThreads('sam1referenceandassortedcommentsfollowing', 'fulldisclosure')
	mergeThreads('sam1referenceandassortedcommentsfollowing', 'fulldisclosureofmethods')
	mergeThreads('sam1referenceandassortedcommentsfollowing', 'fulldisclosureofmethodspatentsandcopyrights')
	mergeThreads('sam1referenceandassortedcommentsfollowing', 'referenceclarificationonsam1')
	mergeThreads('sam1referenceandassortedcommentsfollowing', 'reproducibilitycodesanddistribution')
	mergeThreads('sam1referenceandassortedcommentsfollowing', 'reproduciblitycodesanddistribution')
	mergeThreads('sam1referenceandassortedcommentsfollowing', 'sam1andam1references')
	mergeThreads('sam1referenceandassortedcommentsfollowing', 'sam1paper')
	mergeThreads('sam1referenceandassortedcommentsfollowing', 'sam1reference')
	mergeThreads('sam1referenceandassortedcommentsfollowing', 'sam1referenceam1reference')
	# 2001_813.ods
	mergeThreads('g98wishlistwasgaussian98makefilepolicy', 'gaussian98benchmarkforpcsystems')
	mergeThreads('g98wishlistwasgaussian98makefilepolicy', 'gaussian98makefilepolicy')
	mergeThreads('g98wishlistwasgaussian98makefilepolicy', 'gaussianflamewars')
	mergeThreads('g98wishlistwasgaussian98makefilepolicy', 'gaussianflamewarabalancedview')
	mergeThreads('g98wishlistwasgaussian98makefilepolicy', 'gaussianmakefilepolicy')
	# 2005_336.ods
	mergeThreads('whatopensourcechemistrytoolsdowelackmostwasopensourcecontributionsbypharma', 'whatopensourcechemistrytoolsdowelackmost')
	mergeThreads('whatopensourcechemistrytoolsdowelackmostwasopensourcecontributionsbypharma', 'opensourcecontributionsbypharma')
	# 2005_1588.ods : Should be merge automatically
	# 2006_285.ods : All similar
	# 2008_99.ods
	mergeThreads('glicensewhatyouwantopenwhatyouwantnocomparison', 'licensewhatyouwantopenwhatyouwantnocomparison')
	mergeThreads('glicensewhatyouwantopenwhatyouwantnocomparison', 'glicensingsoftwaretocompetitors')
	mergeThreads('glicensewhatyouwantopenwhatyouwantnocomparison', 'gpcgamessvsgaussian03')
	mergeThreads('glicensewhatyouwantopenwhatyouwantnocomparison', 'gpcgamessvsgussian03')
	
	# Close the log file
	log(f, 'info', 'End threading')
	f.close()