#!/usr/bin/env python
# coding: utf-8

"""A simple spark app in python"""

from pyspark import SparkContext

sc = SparkContext("local[2]", "First Spark App")

file_path = "file:///home/hduser/workspace/repos/spark_demo/data/user_purchase_history.csv"
data = sc.textFile(file_path) \
         .map(lambda line: line.split(",")) \
         .map(lambda record: (record[0], record[1], record[2]))

num_purchases = data.count()
unique_users = data.map(lambda record: record[0]).distinct().count()
total_revenue = data.map(lambda record: float(record[2])).sum()
products = data.map(lambda record: (record[1], 1.0)).reduceByKey(lambda a, b: a+b).collect()
most_popular = sorted(products, key=lambda x: x[1], reverse=True)[0]

print("Total purchases: %d" % num_purchases)
print("Unique users: %d" % unique_users)
print("Total revenue: %2.2f" %total_revenue)
print("Most popular product: %s with %d purchases" % (most_popular[0], most_popular[1]))
