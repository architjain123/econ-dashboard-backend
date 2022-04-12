import sys, csv, json, zipfile, shutil, os
from configparser import ConfigParser
from datetime import datetime, timezone
from pymongo import MongoClient


def upload_data():
    normalization = {}
    with open(norm_file, encoding="utf-8") as patterns_file:
        norm_reader = csv.DictReader(patterns_file, delimiter=',')
        line_count = 0
        for row in norm_reader:
            if line_count != 0 and row["region"] == "ca":
                normalization[int(row["day"])] = int(row["total_devices_seen"])
            line_count += 1

    with open(data_file, encoding="utf-8") as patterns_file:
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

        print("SUCCESS: data uploaded successfully.")


def verify_file_path():

    if len(sys.argv) == 1:
        print("ERROR: Please enter file path in the script.")
        sys.exit(2)

    file_path = sys.argv[1]
    if os.path.exists(file_path):
        return file_path
    else:
        print("ERROR: File path provided does not exist. Please check if file path is correct.")
        sys.exit(2)


def get_year_month_input():

    year = input("Enter the year of the data (Example: 2022): ")
    if not year.isdigit():
        print("ERROR: invalid year entered")
        sys.exit(2)
    month = input("Enter the month of the data (Example: 1): ")
    if not month.isdigit() or (month.isdigit() and (int(month) > 12 or int(month) < 1)):
        print("ERROR: invalid month entered. Month should be between 1-12")
        sys.exit(2)

    print("File set for year: {}, month: {}".format(year, month))
    return int(year), int(month)


def get_dbs():
    config = ConfigParser()
    config.read("config.ini")
    address = config.get("DB", "address")
    username = config.get("DB", "username")
    password = config.get("DB", "password")

    client = MongoClient('mongodb://{}:{}@{}'.format(username, password, address), 27017)
    return client["main"]['patterns'], client["main"]['cache']


def archive_file():
    dest_file_folder = "files"
    dest_file_name = "CORE_PATTERNS_{}_{}.zip".format(year, month)
    dest_file_path = os.path.join(dest_file_folder, dest_file_name)
    try:
        shutil.copy(file, dest_file_path)
        print("Successfully archived file at {}".format(dest_file_path))
    except Exception as e:
        print("ERROR: Couldn't archive file.", e)
        exit(2)


def extract_gz(filename, delete=False):
    import gzip
    with gzip.open(filename, 'rb') as f_in:
        dest = filename.replace(".gz", "")
        with open(dest, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    if delete:
        os.remove(filename)


def decompress_file():
    try:
        if os.path.exists("temp"):
            shutil.rmtree("temp")

        shutil.unpack_archive(filename=file, extract_dir="temp")

        for sub_file in os.listdir("temp"):  # get the list of files
            sub_file_path = os.path.join("temp", sub_file)
            if sub_file_path.endswith(".gz"):  # if it is a zipfile, extract it
                extract_gz(sub_file_path, delete=True)

    except Exception as e:
        print("ERROR: error occurred while decompressing file.", e)
        sys.exit(2)


def get_file_names():

    print("Open the temp folder the select the corresponding numbers.")
    files = os.listdir("temp")
    for idx, sub_file in enumerate(files):
        print(idx, sub_file)

    data_file_idx = input("Enter the number for the data file (usually the largest file): ")
    norm_file_idx = input("Enter the number for the normalization file: ")
    if not data_file_idx.isdigit() or not norm_file_idx.isdigit():
        print("ERROR: enter valid numbers for the files")
        sys.exit(2)

    return os.path.join("temp", files[int(data_file_idx)]), os.path.join("temp", files[int(norm_file_idx)])


def verify_inputs():
    print("\nThe following inputs will be used for file upload:")
    print("Year:", year)
    print("Month:", month)
    print("Data file:", data_file)
    print("Norm file:", norm_file)
    proceed = input("Enter y or yes to continue")
    if proceed.lower() not in ["y", "yes"]:
        print("User did not enter y. Cancelling process.")
        sys.exit(2)


# script start here
if __name__ == "__main__":

    file = verify_file_path()
    year, month = get_year_month_input()

    patterns, cache = get_dbs()
    archive_file()
    decompress_file()
    data_file, norm_file = get_file_names()
    verify_inputs()
    upload_data()

    cache.drop()
    print("Dropped cache.")
