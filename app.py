#!flask/bin/python
from flask import Flask, jsonify
from flask_cors import CORS
from pymongo import MongoClient
from configparser import ConfigParser

config = ConfigParser()
config.read("config.txt")
address = config.get("DB", "address")
username = config.get("DB", "username")
password = config.get("DB", "password")

app = Flask(__name__)
CORS(app)

client = MongoClient('mongodb://{}:{}@{}'.format(username, password, address), 27017)
db = client["main"]
patterns = db['patterns']
cache = db['cache']


@app.route('/get/overall', methods=['GET'])
def overall():
    # cursor = patterns.aggregate([{"$group": {"_id": "$date", "total": {"$sum": "$visits"}}},\
    #                              {"$sort": {"_id": 1}}])
    # result = []
    # for obj in cursor:
    #     result.append([obj["_id"]*1000, obj["total"]])
    # cache.insert_one({"type": "overall", "data": result})

    obj = cache.find_one({"type": "overall"})
    return jsonify({"data": obj["data"]})


@app.route('/get/naics/<naics_code>', methods=['GET'])
def naics(naics_code):
    cursor = patterns.aggregate([{"$match": {"naics_code": {"$regex": '^' + naics_code}}},
                                 {"$group": {"_id": "$date", "total": {"$sum": "$visits"}}},
                                 {"$sort": {"_id": 1}}])
    result = []
    for obj in cursor:
        result.append([obj["_id"] * 1000, obj["total"]])
    return jsonify({"data": result})


@app.route('/get/postal/<postal_code>', methods=['GET'])
def postal(postal_code):
    cursor = patterns.aggregate([{"$match": {"postal_code": postal_code}},
                                 {"$group": {"_id": "$date", "total": {"$sum": "$visits"}}},
                                 {"$sort": {"_id": 1}}])
    result = []
    for obj in cursor:
        result.append([obj["_id"] * 1000, obj["total"]])
    return jsonify({"data": result})


@app.route('/get/name/<business_name>', methods=['GET'])
def name(business_name):
    cursor = patterns.aggregate([{"$match": {"location_name": business_name}},
                                 {"$group": {"_id": "$date", "total": {"$sum": "$visits"}}},
                                 {"$sort": {"_id": 1}}])
    result = []
    for obj in cursor:
        result.append([obj["_id"] * 1000, obj["total"]])
    return jsonify({"data": result})


@app.route('/get/open_businesses', methods=['GET'])
def open_businesses():
    # cursor = patterns.aggregate([{"$match":{}},
    #                              {"$group": {"_id": "$date",
    #                                          "open": {"$sum": {"$cond": [{"$gte":["$visits", 1]}, 1, 0]}},
    #                                          "total": {"$sum": 1}}},
    #                              {"$sort": {"_id": 1}}])
    #
    # result = []
    # for obj in cursor:
    #     result.append([obj["_id"]*1000, obj["open"]/obj["total"]])
    # cache.insert_one({"type": "open_businesses", "data": result})
    obj = cache.find_one({"type": "open_businesses"})
    return jsonify({"data": obj["data"]})

if __name__ == '__main__':
    app.run(debug=True)