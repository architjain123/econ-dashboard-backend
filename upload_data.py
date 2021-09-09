import csv
import json
from configparser import ConfigParser
from datetime import datetime
from pymongo import MongoClient
import zipfile

config = ConfigParser()
config.read("config.txt")
address = config.get("DB", "address")
username = config.get("DB", "username")
password = config.get("DB", "password")

client = MongoClient('mongodb://{}:{}@{}'.format(username, password, address), 27017)
db = client["main"]
patterns = db['patterns']

filename = "CA-CORE_POI-PATTERNS-2021_06-2021-07-29.zip"
year = 2021
month = 6

with zipfile.ZipFile("files/" + filename, 'r') as zip_ref:
    zip_ref.extractall("temp")

csvfile = "temp/core_poi-patterns.csv"
normfile = "temp/normalization_stats.csv"

normalization = {}
with open(normfile, encoding="utf-8") as patterns_file:
    norm_reader = csv.DictReader(patterns_file, delimiter=',')
    line_count = 0
    for row in norm_reader:
        if line_count != 0 and row["region"] == "ca":
            normalization[int(row["day"])] = int(row["total_visits"])
        line_count += 1

with open(csvfile, encoding="utf-8") as patterns_file:
    csv_reader = csv.DictReader(patterns_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        if line_count != 0 and row["visits_by_day"]:
            docs = []
            for idx, val in enumerate(json.loads(row["visits_by_day"])):
                doc = {"placekey": row["placekey"], "location_name": row["location_name"], "naics_code": row["naics_code"], "postal_code": row["postal_code"], "date": datetime(year, month, idx+1), "visits": val, "normalization": normalization[idx+1]}
                docs.append(doc)
            patterns.insert_many(docs)
        line_count += 1
        if line_count % 100 == 0:
            print(line_count)