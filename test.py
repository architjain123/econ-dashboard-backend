from ConfigManager import ConfigManager
from pymongo import MongoClient
from datetime import datetime, timezone

config = ConfigManager("config.txt")
address, username, password = config.get_db("address"), config.get_db("username"), config.get_db("password")

client = MongoClient('mongodb://{}:{}@{}'.format(username, password, address), 27017)
db = client["main"]
patterns = db['patterns']

# averages = [0,0,0,0,0,0,0]
# count = [0,0,0,0,0,0,0]
#
# cursor = patterns.aggregate([{"$match":{"date": {"$lt": 1580515200}}},
#                              {"$group": {"_id": "$date",
#                                          "open": {"$sum": {"$cond": [{"$gte":["$visits", 1]}, 1, 0]}},
#                                          "total": {"$sum": 1}}},
#                              {"$sort": {"_id": 1}}])
#
# result = []
# for obj in cursor:
#     weekday = datetime.fromtimestamp(obj["_id"], tz=timezone.utc).weekday()
#     averages[weekday] += obj["open"]/obj["total"]
#     count[weekday] += 1
#
# jan20_averages = [averages[i]/count[i] for i in range(7)]
#
# print(jan20_averages)

cursor = patterns.aggregate([{"$match":{"date": {"$lt": 1580515200}}},
                             {"$group": {
        "_id": "$date",
        "total": {"$sum": {"$divide": ["$visits", "$total_devices_seen"]}}}},
        {"$sort": {"_id": 1}}])
average = 0
count = 0
for obj in cursor:
    average += obj["total"]
    count += 1

janAverage = average/count
print(janAverage)
