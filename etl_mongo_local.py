import os
import csv
import json
import pandas as pd
import sys, getopt, pprint
from pymongo import MongoClient

#CSV to JSON Conversion
csvfile = open('C://Users//aipat//GitHubPesh//ETL_Project//etl_congressional//OlympicsGDP.csv', 'r')
reader = csv.DictReader( csvfile )

client = MongoClient('mongodb://localhost:27017')

# Delcare Database
db = client.OlympicsGDP_db

# Declare the collection
collection = db.OlympicsGDP_db

db.segment.drop()

header = [ "Country",	"Total_Medals",	"Population",	"GDP ($ per capita)"]

for each in reader:
    row={}
    for field in header:
        row[field]=each[field]

    db.segment.insert(row)