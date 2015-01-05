#!/usr/bin/env python
# -*- coding: utf-8 -*-
# python cleaning.py


# Imports
import datetime, io, re, time
from dateutil import parser
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


def rewriteEmail(email):
	email = email.lower()
	regex = '('
	regex += '\a/'
	regex += '|at\.at'
	regex += '|\.\-at\-\.'
	regex += '|---'
	regex += '|--'
	regex += '|###'
	regex += '|##'
	regex += '|:-:'
	regex += '|\|\|'
	regex += '|\(!\)'
	regex += '|-\$-'
	regex += '|\^-\^'
	regex += '|\+\+'
	regex += '|\!a\!'
	regex += '|==='
	regex += '|\{=\}'
	regex += '|\[a\]'
	regex += '|\]!\['
	regex += '|\(a\)'
	regex += '|\]~\['
	regex += '|~~'
	regex += '|,\+,'
	regex += '|\]\^\['
	regex += '|\*\*'
	regex += '|/\./'
	regex += '|~!~'
	regex += '|\[\*\]'
	regex += '|_\+_'
	regex += '|\(\)'
	regex += '|\[-\]'
	regex += '|\],\['
	regex += '|\.-at\.-'
	regex += '|\'at\\\`'
	regex += '|\.:\.'
	regex += '|\]\-\['
	regex += '|:_:'
	regex += '|#,#'
	regex += '|\]=\['
	regex += '|\*_\*'
	regex += '|\+/\-'
	regex += '|-#-'
	regex += '|\[#\]'
	regex += '|#%#'
	regex += '|\-x\-'
	regex += '|::'
	regex += '|\*\|\*'
	regex += '|\^\^'
	regex += '|,,'
	regex += '|!at!'
	regex += '|\.\.at\.\.'
	regex += '|~at~'
	regex += '|;;'
	regex += '|-\*-'
	regex += '|\]"\['
	regex += '|\]\|\['
	regex += '|\]\*\['
	regex += '|\(_\)'
	regex += '|%%'
	regex += '|\|a\|'
	regex += '|=-='
	regex += '|_\._'
	regex += '|-,-'
	regex += '|\(0\)'
	regex += '|\(-\)'
	regex += '|_at_'
	regex += '|_\(\)_'
	regex += '|_\(a\)_'
	regex += '|\)at\('
	regex += '|\*at\*'
	regex += '|=at='
	regex += '|/at/'
	regex += '|&lt;&lt;at&gt;&gt;'
	regex += '|&lt;at&gt;'
	regex += '|&lt;&gt;'
	regex += '|;&gt;'
	regex += '|@@@'
	regex += '|@x@'
	regex += '|@=@'
	regex += '|@v@'
	regex += '|@a@'
	regex += '|@o@'
	regex += '|@@'
	regex += '|{}'
	regex += '|\[\]'
	regex += '|\]_\['
	regex += '|%a%'
	regex += '|\!v\!'
	regex += '|\!=\!'
	regex += '|\!\!'
	regex += '|\|\*\|'
	regex += '|\|,\|'
	regex += '|%x%'
	regex += '|:a:'
	regex += '|\*o\*'
	regex += '|\!\^\!'
	regex += '|\|"\|'
	regex += '|:\+:'
	regex += '|_\-_'
	regex += '|\|\-\|'
	regex += '|\*&amp;\*'
	regex += '|\-&amp;\-'
	regex += '|\^_\^'
	regex += '|\.\.:\.\.'
	regex += '|\.\.\.\.'
	regex += '|\.\.'
	regex += '|__'
	regex += '|\"\"'
	regex += '|-\.-'
	regex += '|&lt;&gt;'
	regex += '|&gt;&lt;'
	regex += '|&lt;\*&gt;'
	regex += '|&gt;\*&lt;'
	regex += '|&amp;at&amp;'
	regex += '|,;,'
	regex += '|\(~\)'
	regex += '|\{:\}'
	regex += ')'
	email = re.sub(r'[\s\n]', r'', email)
	emailTmp = email
	if emailTmp == email:
		emailTmp = re.sub(regex, r'@', email)
	if emailTmp == email:
		emailTmp = re.sub(r'\\a/', r'@', email)
	if emailTmp == email:
		emailTmp = re.sub(r'/a\\', r'@', email)
	if emailTmp == email:
		emailTmp = re.sub(r'\((.*)at(.*)\)', r'@', email)
	if emailTmp == email:
		emailTmp = re.sub(r'-x-(.*)at(.*)-x-', r'@', email)
	if emailTmp == email:
		emailTmp = re.sub(r'\%\-\%(.*)at(.*)\%\-\%', r'@', email)
	if emailTmp == email:
		emailTmp = re.sub(r'\&(.*)at(.*)\&', r'@', email)
	if emailTmp == email:
		emailTmp = re.sub(r'\^(.*?)at(.*?)\^', r'@', email)
	if emailTmp == email:
		emailTmp = re.sub(r'\$(.*)at(.*)\$', r'@', email)
	if emailTmp == email:
		emailTmp = re.sub(r'\#(.*)at(.*)\#', r'@', email)
	if emailTmp == email:
		emailTmp = re.sub(r'\|(.*)at(.*)\|', r'@', email)
	if emailTmp == email:
		emailTmp = re.sub(r'\{(.*)at(.*)\}', r'@', email)
	if emailTmp == email:
		emailTmp = re.sub(r'\"(.*?)at(.*?)\"', r'@', email)
	if emailTmp == email:
		emailTmp = re.sub(r'\-(.*?)at(.*?)\-', r'@', email)
	if emailTmp == email:
		emailTmp = re.sub(r'\:(.*)at(.*)\:', r'@', email)
	if emailTmp == email:
		emailTmp = re.sub(r'\%(.*?)at(.*?)\%', r'@', email)
	if emailTmp == email:
		emailTmp = re.sub(r'\\(.*)at(.*)\/', r'@', email)
	if emailTmp == email:
		emailTmp = re.sub(r'\;(.*?)at(.*?)\;', r'@', email)
	#if emailTmp == email:
	#	emailTmp = re.sub(r'\.(.*?)at(.*?)\.', r'@', email)
	if emailTmp == email:
		emailTmp = re.sub(r'\[(.*?)at(.*?)\]', r'@', email)
	if emailTmp == email:
		emailTmp = re.sub(r'#|%|!|\^|~|,|:|\*|=|\|', r'@', email)
	return emailTmp


# Iterate over mails
def cleanData(mails):
	data = mails.find()
	for d in data:
		# Get current id
		fieldId = d['_id']
		# Create new field timestamp from "date" field
		if 'date' in d:
			if re.search(u'CCL (.*)', d['date']):
				fieldDate = re.search(u'CCL (.*)', d['date']).group(1)
			else:
				fieldDate = d['date']
		else:
			fieldDate = ''
		fieldTimestamp = int(time.mktime(parser.parse(fieldDate).timetuple()))
		# Transform subject field
		if 'subject' in d:
			fieldSubject = re.sub(r'Subject: ', r'', d['subject'])
		else:
			fieldSubject = ''
		# Create new field email from "from" field
		if 'from' in d:
			fieldFrom = re.sub(r'From: ', r'', d['from'])
			fieldEmail = d['from'].lower()
		if re.search('href=\"mailto:(.*?)\" ', fieldEmail):
			fieldEmail = re.search('href=\"mailto:(.*?)\" target', fieldEmail).group(1)
			fieldEmail = re.sub(r'%40', r'@', fieldEmail)
		elif re.search(r'&lt;(.*)&gt;', fieldEmail):
			emailaddress = re.search(r'&lt;(.*)&gt;', fieldEmail).group(1)
			if emailaddress != '':
				emailaddress2 = rewriteEmail(emailaddress)
				if emailaddress2.count('@') != 1: 
					emailaddress2 = re.sub(r'( a | \. | _ )', r'@', emailaddress)
					if emailaddress2.count('@') != 1:
						fieldEmail = re.sub(r'<em>from</em>: ', r'', fieldEmail)
						fieldEmail = re.sub(r'[^\w]', r'', fieldEmail)
					else:
						fieldEmail = emailaddress2
				else:
					fieldEmail = emailaddress2
			else:
				fieldEmail = re.sub(r'<em>from</em>: ', r'', fieldEmail)
				fieldEmail = re.sub(r'[^\w]', r'', fieldEmail)
		else:
			fieldEmail = re.sub(r'<em>from</em>: ', r'', fieldEmail)
			fieldEmail = re.sub(r'[^\w]', r'', fieldEmail)
		fieldEmailLength = len(fieldEmail)
		fieldXMessageId = ''
		fieldXReference = ''
		if 'comments' in d:
			for fieldComment in d['comments']:
				if 'X-Message-Id' in fieldComment:
					fieldXMessageId = rewriteEmail(re.sub(r'X-Message-Id:\n? ', r'', fieldComment))
				elif 'X-Reference' in fieldComment:
					fieldXReference = rewriteEmail(re.sub(r'X-Reference:\n? ', r'', fieldComment))
		if 'content' in d:
			fieldContentClean = d['content']
			# Remove every line beginning by ">"
			fieldContentClean = re.sub(r' *>(.*?)\n *', r'', fieldContentClean)
			# Remove every line beginning by "To: "
			fieldContentClean = re.sub(r' *To:(.*?)\n *', r'', fieldContentClean)
			# Remove every line beginning by "Subject: "
			fieldContentClean = re.sub(r' *Subject:(.*?)\n *', r'', fieldContentClean)
			# Remove every line beginning by "Date: "
			fieldContentClean = re.sub(r' *Date:(.*?)\n *', r'', fieldContentClean)
			# Remove every line beginning by "From: "
			fieldContentClean = re.sub(r' *From:(.*?)\n *', r'', fieldContentClean)
			# Remove every line beginning by "Status: "
			fieldContentClean = re.sub(r' *Status:(.*?)\n *', r'', fieldContentClean)
			# Remove every line beginning by "Status: "
			fieldContentClean = re.sub(r'<blockquote(.*)/blockquote>', r'', fieldContentClean, 0, re.DOTALL)
		mails.update(
			{'_id': fieldId},
			{
				'$set': {
					'date': fieldDate,
					'subject': fieldSubject,
					'from': fieldFrom,
					'email': fieldEmail,
					'emaillength' : fieldEmailLength,
					'timestamp': fieldTimestamp,
					'xmessageid': fieldXMessageId,
					'xreference': fieldXReference,
					'contentclean': fieldContentClean
				}
			}
		)


# Main
if __name__ == "__main__":
	# MongoDB connection
	client = MongoClient('localhost', 27017)
	db = client.ccl
	mails = db.mails

	# Open a log file
	f = io.open('cleaning.log', 'w', encoding='utf-8')
	log(f, 'info', 'Start cleaning')

	# Clean and update mails
	data = cleanData(mails)

	# Close the log file
	log(f, 'info', 'End cleaning')
	f.close()