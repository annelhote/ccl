#!/usr/bin/env python
# -*- coding: utf-8 -*-
# python cleaning.py


# Imports
import re, sys, json, time, codecs
from dateutil import parser
from pprint import pprint
from pymongo import MongoClient


def rewriteEmail(email):
	email = email.lower()
	email = re.sub(r'[\s\n]', r'', email)
	emailTmp = re.sub(r'at\.at', r'@', email)
	if emailTmp == email:
		emailTmp = re.sub(r'\'at\\\`', r'@', email)
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
	if emailTmp == email:
		emailTmp = re.sub(r'\.(.*?)at(.*?)\.', r'@', email)
	if emailTmp == email:
		emailTmp = re.sub(r'\[(.*?)at(.*?)\]', r'@', email)
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
			before = d['from']
			fieldFrom = re.sub(r'From: ', r'', d['from'])
			fieldEmail = d['from'].lower()
		if re.search('href=\"mailto:(.*?)\" ', fieldEmail):
			fieldEmail = re.search('href=\"mailto:(.*?)\" target', fieldEmail).group(1)
			fieldEmail = re.sub(r'%40', r'@', fieldEmail)
		else:
			fieldEmail = re.sub(r'<em>from</em>: ', r'', fieldEmail)
			fieldEmail = re.sub(r'[^\w]', r'', fieldEmail)
		fieldXMessageId = ''
		fieldXReference = ''
		if 'comments' in d:
			for fieldComment in d['comments']:
				if 'X-Message-Id' in fieldComment:
					fieldXMessageId = rewriteEmail(re.sub(r'X-Message-Id:\n? ', r'', fieldComment))
				elif 'X-Reference' in fieldComment:
					fieldXReference = rewriteEmail(re.sub(r'X-Reference:\n? ', r'', fieldComment))
		mails.update(
			{'_id': fieldId},
			{
				'$set': {
					'date': fieldDate,
					'subject': fieldSubject,
					'from': fieldFrom,
					'email': fieldEmail,
					'timestamp': fieldTimestamp,
					'X-Message-Id': fieldXMessageId,
					'X-Reference': fieldXReference
				}
			}
		)


# Main
if __name__ == "__main__":
	# MongoDB connection
	client = MongoClient('localhost', 27017)
	db = client.ccl
	mails = db.mails

	# Clean and update mails
	data = cleanData(mails)