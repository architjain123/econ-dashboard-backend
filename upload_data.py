import csv, json, re, zipfile, shutil, gzip
from configparser import ConfigParser
from datetime import datetime, timezone
from pymongo import MongoClient
from os import walk


def get_files_to_parse():
    files_to_parse = []
    with open('parsed_files.json', 'r') as f:
        json_file = json.loads(f.read())
        parsed_files = set(json_file["files"])
        files_in_dir = next(walk("files"), (None, None, []))[2]

        for filename in files_in_dir:
            if filename not in parsed_files and filename.startswith("CA-CORE_POI-PATTERNS"):
                files_to_parse.append(filename)

    return files_to_parse


def parse_year_month(filename):
    filesplit = filename.split("-")[3]
    year = filesplit[:4]
    month = filesplit[-2:]
    return int(year), int(month)


def unzipGzFile(gz_file):
    try:
        with gzip.open(gz_file, 'rb') as f_in:
            with open(gz_file.replace('.gz', ""), 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    except:
        return


def parse_upload_file(filename, patterns):

    shutil.rmtree("temp")
    with zipfile.ZipFile("files/" + filename, 'r') as zip_ref:
        zip_ref.extractall("temp")

    unzipGzFile("temp/core_poi-patterns.csv.gz")
    csv_file = "temp/core_poi-patterns.csv"
    norm_file = "temp/normalization_stats.csv"

    normalization = {}
    with open(norm_file, encoding="utf-8") as patterns_file:
        norm_reader = csv.DictReader(patterns_file, delimiter=',')
        line_count = 0
        for row in norm_reader:
            if line_count != 0 and row["region"] == "ca":
                normalization[int(row["day"])] = int(row["total_visits"])
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
                           "visits": val, "normalization": normalization[idx + 1]}
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


def add_to_parsed_files(filename):
    with open('parsed_files.json', 'r+') as f:
        data = json.load(f)
        data['files'].append(filename)
        f.seek(0)  # reset file position to the beginning.
        json.dump(data, f, indent=4)
        f.truncate()  # remove remaining part


config = ConfigParser()
config.read("config.txt")
address, username, password = config.get("DB", "address"), config.get("DB", "username"), config.get("DB", "password")

client = MongoClient('mongodb://{}:{}@{}'.format(username, password, address), 27017)
patterns = client["main"]['patterns']

files_to_parse = get_files_to_parse()
if not files_to_parse:
    print("No files to parse")
    quit()
else:
    print("Parsing files : ", files_to_parse)

for filename in files_to_parse:
    year, month = parse_year_month(filename)
    print("Parsing", filename)
    if parse_upload_file(filename, patterns):
        add_to_parsed_files(filename)
    else:
        print("Unable to upload file: " + filename)