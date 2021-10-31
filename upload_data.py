import csv, json, zipfile, shutil
from ConfigManager import ConfigManager
from datetime import datetime, timezone
from pymongo import MongoClient
from FileManager import FileManager


def parse_upload_file(filename, patterns):

    shutil.rmtree("temp")
    with zipfile.ZipFile("files/" + filename, 'r') as zip_ref:
        zip_ref.extractall("temp")

    FileManager.unzip_gz_file("temp/core_poi-patterns.csv.gz")
    csv_file = "temp/core_poi-patterns.csv"
    norm_file = "temp/normalization_stats.csv"

    normalization = {}
    with open(norm_file, encoding="utf-8") as patterns_file:
        norm_reader = csv.DictReader(patterns_file, delimiter=',')
        line_count = 0
        for row in norm_reader:
            if line_count != 0 and row["region"] == "ca":
                normalization[int(row["day"])] = int(row["total_devices_seen"])
            line_count += 1

    with open(csv_file, encoding="utf-8") as patterns_file:
        csv_reader = csv.DictReader(patterns_file, delimiter=',')
        line_count = 0
        documents = []
        for row in csv_reader:
            if line_count != 0 and row["visits_by_day"]:
                for idx, val in enumerate(json.loads(row["visits_by_day"])):
                    date = int(datetime(year, month, idx + 1).replace(tzinfo=timezone.utc).timestamp())
                    doc = {"placekey": row["placekey"], "location_name": row["location_name"],
                           "naics_code": row["naics_code"], "postal_code": row["postal_code"], "date": date,
                           "visits": val, "total_devices_seen": normalization[idx + 1]}
                    documents.append(doc)

            line_count += 1
            if line_count % 1000 == 0:
                print(line_count)

            if len(documents) > 99000:
                patterns.insert_many(documents)
                documents = []

        if len(documents) > 0:
            patterns.insert_many(documents)

        return True


config = ConfigManager("config.txt")
address, username, password = config.get_db("address"), config.get_db("username"), config.get_db("password")

client = MongoClient('mongodb://{}:{}@{}'.format(username, password, address), 27017)
patterns = client["main"]['patterns']

files_to_parse = FileManager.get_files_to_parse()
if not files_to_parse:
    print("No files to parse")
    quit()
else:
    print("Parsing files : ", files_to_parse)

for filename in files_to_parse:
    year, month = FileManager.parse_year_month(filename)
    print("Parsing", filename)
    if parse_upload_file(filename, patterns):
        FileManager.add_to_parsed_files(filename)
    else:
        print("Unable to upload file: " + filename)