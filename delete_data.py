import sys, calendar
from configparser import ConfigParser
from datetime import datetime, timezone
from pymongo import MongoClient


def get_year_month_input():

    year = input("Enter the year of the data (Example: 2022): ")
    if not year.isdigit():
        print("ERROR: invalid year entered")
        sys.exit(2)
    month = input("Enter the month of the data (Example: 1): ")
    if not month.isdigit() or (month.isdigit() and (int(month) > 12 or int(month) < 1)):
        print("ERROR: invalid month entered. Month should be between 1-12")
        sys.exit(2)

    print("Data to be deleted set for year: {}, month: {}".format(year, month))
    return int(year), int(month)


def get_dbs():
    config = ConfigParser()
    config.read("config.ini")
    address = config.get("DB", "address")
    username = config.get("DB", "username")
    password = config.get("DB", "password")

    client = MongoClient('mongodb://{}:{}@{}'.format(username, password, address), 27017)
    return client["main"]['patterns'], client["main"]['cache']


def verify_inputs():
    print("\nThe data for the following month will be deleted:")
    print("Year:", year)
    print("Month:", month)
    proceed = input("Enter y or yes to continue: ")
    if proceed.lower() not in ["y", "yes"]:
        print("User did not enter y or yes. Cancelling process.")
        sys.exit(2)


def delete_data():

    start_day, end_day = calendar.monthrange(year, month)
    date_start = int(datetime(year, month, start_day).replace(tzinfo=timezone.utc).timestamp())
    date_end = int(datetime(year, month, end_day).replace(tzinfo=timezone.utc).timestamp())
    count = patterns.delete_many({"date": {"$gte": date_start, "$lt": date_end}})
    print(count, "documents deleted")


# script start here
if __name__ == "__main__":

    year, month = get_year_month_input()

    patterns, cache = get_dbs()
    verify_inputs()
    delete_data()

    cache.drop()
    print("Dropped cache.")