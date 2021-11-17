#!flask/bin/python
from flask import Flask, jsonify
from flask_cors import CORS
from pymongo import MongoClient
from configparser import ConfigParser
from datetime import datetime, timezone

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
    # janAverage = 0.049820101400157545
    # cursor = patterns.aggregate([{"$group": {
    #     "_id": "$date",
    #     "total": {"$sum": {"$divide": ["$visits", "$total_devices_seen"]}}}},
    #     {"$sort": {"_id": 1}}])
    # result = []
    # for obj in cursor:
    #     result.append([obj["_id"]*1000, obj["total"]/janAverage])
    # cache.insert_one({"type": "overall_foot", "data": result})
    result = cache.find_one({"type": "overall_foot"})["data"]
    return jsonify({"data": result})


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
    # jan20_avgs = [0.7220641489912054, 0.7354500775995862, 0.7075530263838592, 0.7343162614243834, 0.7442317640972581, 0.6770994999137783, 0.6347215037075358]
    # cursor = patterns.aggregate([{"$match":{}},
    #                              {"$group": {"_id": "$date",
    #                                          "open": {"$sum": {"$cond": [{"$gte":["$visits", 1]}, 1, 0]}},
    #                                          "total": {"$sum": 1}}},
    #                              {"$sort": {"_id": 1}}])
    #
    # result = []
    # for obj in cursor:
    #     weekday = datetime.fromtimestamp(obj["_id"], tz=timezone.utc).weekday()
    #     result.append([obj["_id"]*1000, (obj["open"]/obj["total"])/jan20_avgs[weekday]])
    # cache.insert_one({"type": "open_businesses", "data": result})
    result = cache.find_one({"type": "open_businesses"})["data"]
    return jsonify({"data": result})


@app.route('/get/retail_open_businesses', methods=['GET'])
def retail_open_businesses():
    # jan20_avgs = [0.80396865520728, 0.8121840242669363, 0.7784630940343782, 0.8120323559150657, 0.8253791708796765, 0.7642821031344793, 0.7024772497472194]
    # cursor = patterns.aggregate([{"$match": {"naics_code": {"$regex": '^44'}}},
    #                              {"$group": {"_id": "$date",
    #                                          "open": {"$sum": {"$cond": [{"$gte":["$visits", 1]}, 1, 0]}},
    #                                          "total": {"$sum": 1}}},
    #                              {"$sort": {"_id": 1}}])
    #
    # result = []
    # for obj in cursor:
    #     weekday = datetime.fromtimestamp(obj["_id"], tz=timezone.utc).weekday()
    #     result.append([obj["_id"]*1000, (obj["open"]/obj["total"])/jan20_avgs[weekday]])
    # cache.insert_one({"type": "retail_open_businesses", "data": result})
    result = cache.find_one({"type": "retail_open_businesses"})["data"]
    return jsonify({"data": result})


@app.route('/get/lnh_open_businesses', methods=['GET'])
def lnh_open_businesses():
    # jan20_avgs = [0.8950829940494832, 0.8995458816160351, 0.8846226119636705, 0.9036642655809584, 0.9207015346069529, 0.9044002505480739, 0.8753523332289382]
    # cursor = patterns.aggregate([{"$match": {"naics_code": {"$regex": '^7'}}},
    #                              {"$group": {"_id": "$date",
    #                                          "open": {"$sum": {"$cond": [{"$gte":["$visits", 1]}, 1, 0]}},
    #                                          "total": {"$sum": 1}}},
    #                              {"$sort": {"_id": 1}}])
    # result = []
    # for obj in cursor:
    #     weekday = datetime.fromtimestamp(obj["_id"], tz=timezone.utc).weekday()
    #     result.append([obj["_id"]*1000, (obj["open"]/obj["total"])/jan20_avgs[weekday]])
    # cache.insert_one({"type": "lnh_open_businesses", "data": result})
    result = cache.find_one({"type": "lnh_open_businesses"})["data"]
    return jsonify({"data": result})


if __name__ == '__main__':
    app.run(debug=True)