#!/usr/bin/env python

from pymongo import MongoClient
from glob import glob

connection = MongoClient()

db = connection.db

gaia = db.gaia


counter = 0
for csv in glob("/Volumes/Gaia Data/*.csv"):
    print csv
    for line in open(csv, "r"):
        tokens = line.split('|')
        if len(tokens) != 48:
            print "Occhio, meno di 48: %s" % line
        
        document = dict()
        for numero in xrange(len(tokens)):
            nomeCampo= "%d" % numero
            document[nomeCampo] = tokens[numero]

        gaia.insert(document)
        counter = counter + 1
        if(counter % 1000 == 0):
            print "Inseriti %d documenti" % counter

connection.close()
