from datetime import datetime, timezone


def get_weekly_averages_jan2020(collection, naics_code=""):
    cursor = collection.aggregate([{"$match":{"date": {"$lt": 1580515200}, "naics_code": {"$regex": '^' + naics_code}}},
                                 {"$group": {"_id": "$date",
                                             "open": {"$sum": {"$cond": [{"$gte":["$visits", 1]}, 1, 0]}},
                                             "total": {"$sum": 1}}},
                                 {"$sort": {"_id": 1}}])

    averages, count = [0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0]
    for obj in cursor:
        weekday = datetime.fromtimestamp(obj["_id"], tz=timezone.utc).weekday()
        averages[weekday] += obj["open"]/obj["total"]
        count[weekday] += 1

    jan_weekly_averages = [averages[i]/count[i] for i in range(7)]
    return jan_weekly_averages


def get_monthly_average_jan2020(collection, naics_code=""):
    cursor = collection.aggregate([{"$match":{"date": {"$lt": 1580515200}, "naics_code": {"$regex": '^' + naics_code}}},
                                 {"$group": {
            "_id": "$date",
            "total": {"$sum": {"$divide": ["$visits", "$total_devices_seen"]}}}},
            {"$sort": {"_id": 1}}])

    average, count = 0, 0
    for obj in cursor:
        average += obj["total"]
        count += 1

    jan_average = average/count
    return jan_average
