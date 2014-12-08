#!/usr/bin/env python
# -*- coding: utf-8 -*-
# python graph.py


# Imports
import networkx as nx
from gexf import Gexf, GexfImport
from pymongo import MongoClient
from pprint import pprint
import random


# Light blue
authorColor = {
	'r' : 0,
	'g' : 102,
	'b' : 255
}
# Light pink
threadColor = {
	'r' : 255,
	'g' : 0,
	'b' : 102
}


# Main
if __name__ == "__main__":
	# MongoDB connection
	client 		= MongoClient('localhost', 27017)
	db 			= client.ccl
	mails 		= db.mails
	threads 	= db.threads
	authors		= db.authors

	# Create a graph
	g = nx.Graph()

	# Create a graph
	gexf 		= Gexf("Anne L'Hote","A bipartite graph of threads vs. authors")
	graph 		= gexf.addGraph("directed","static","ccl")

	output_file	= open("data/ccl.gexf","w")

	for author in authors.find():
		# Add a node by author
		g.add_node(str(author['_id']), {
			'viz': {
				'color': {'r': authorColor['r'], 'g': authorColor['g'], 'b': authorColor['b']},
				'position': {'y': random.random(), 'x': random.random(), 'z': 0.0},
				'size': author['count']},
			'label': author['email']
		})

	for thread in threads.find({'count':{'$gte':3}}):
		# Add a node by thread
		g.add_node(str(thread['_id']), {
			'viz': {
				'color': {'r': threadColor['r'], 'g': threadColor['g'], 'b': threadColor['b']},
				'position': {'y': random.random(), 'x': random.random(), 'z': 0.0},
				'size': thread['count']},
			'label': thread['title']
		})
		# Add a link between this thread and each author of each mail of this thread
		for mail in thread['mails']:
			emailAdress = mails.find({'_id':mail})[0]
			# Retrieve author id
			a = authors.find({'email':emailAdress['email']},{'_id':1})[0]
			g.add_edge(str(thread['_id']), str(a['_id']))

	# Write a graph
	nx.write_gexf(g, "data/ccl.gexf")