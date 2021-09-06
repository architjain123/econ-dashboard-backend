import csv
import json
from datetime import datetime
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client["main"]
patterns = db['patterns']

filename = "CA-CORE_POI-PATTERNS-2021_06-2021-07-29/core_poi-patterns.csv"
norm = "CA-CORE_POI-PATTERNS-2021_06-2021-07-29/normalization_stats.csv"

year = 2021
month = 6

with open(filename, encoding="mbcs") as patterns_file:
    csv_reader = csv.DictReader(patterns_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            line_count+=1
            continue
        else:
            if row["visits_by_day"]:
                docs = []
                for idx, val in enumerate(json.loads(row["visits_by_day"])):
                    doc = {"placekey": row["placekey"], "location_name": row["location_name"], "naics_code": row["naics_code"], "postal_code": row["postal_code"], "date": datetime(year, month, idx+1), "visits": val}
                    docs.append(doc)
                patterns.insert_many(docs)
