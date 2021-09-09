#!flask/bin/python
from flask import Flask, jsonify
from pymongo import MongoClient
from configparser import ConfigParser
config = ConfigParser()
config.read("config.txt")
address = config.get("DB", "address")
username = config.get("DB", "username")
password = config.get("DB", "password")

app = Flask(__name__)

client = MongoClient('mongodb://{}:{}@{}'.format(username, password, address), 27017)
db = client["main"]
patterns = db['patterns']

@app.route('/get/graphdata/allfoot', methods=['GET'])
def get_all_foottraffic():

    cursor = patterns.aggregate([{"$group": {"_id": "$date", "total": { "$sum": "$visits" }}},\
                                 {"$sort": {"_id": 1}}])
    result = []
    for obj in cursor:
        result.append({"date": str(obj["_id"].isoformat()), "total": obj["total"]})
    return jsonify(result)


if __name__ == '__main__':
    app.run(debug=True)