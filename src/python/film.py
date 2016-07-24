"""A simple spark app in python"""

from pyspark import SparkContext


import numpy as np
import pandas as pd
import matplotlib as mpl
mpl.use("Agg")
import matplotlib.pyplot as plt
import os
sc = SparkContext("local[2]", "C3")

# user data
user_data = sc.textFile('file://' + os.path.abspath('../data/ml-100k/') + '/u.user')
user_fields = user_data.map(lambda line: line.split("|"))
num_users = user_fields.map(lambda fields: fields[0]).distinct().count()
num_genders = user_fields.map(lambda fields: fields[2]).distinct().count()
num_occupations = user_fields.map(lambda fields: fields[3]).distinct().count()
num_zipcodes = user_fields.map(lambda fields: fields[4]).distinct().count()
print("users: %d, genders: %d, occupations: %d, ZIP codes: %d" %
      (num_users, num_genders, num_occupations, num_zipcodes))


# ages = user_fields.map(lambda x: int(x[1])).collect()
# plt.hist(ages, bins=20, color='lightblue', normed=True); plt.savefig("file:///home/hotelbi/wanggang/.doc/images/ages.png")

count_by_occupation1 = user_fields.map(lambda fields: (
    fields[3], 1)).reduceByKey(lambda x, y: x + y).collect()
count_by_occupation2 = user_fields.map(lambda fields: fields[3]).countByValue()
print("Map-reduce method:")
print(dict(count_by_occupation2))
print("countByValue method:")
print(dict(count_by_occupation2))


# Movie dataset
movie_data = sc.textFile('file://' + os.path.abspath('../data/ml-100k/') + '/u.item')
print(movie_data.first())
num_movies = movie_data.count()
print("Movies: %d" % num_movies)


def convert_year(x):
    try:
        return int(x[-4:])
    except:
        return 1900
movie_fields = movie_data.map(lambda lines: lines.split("|"))
years = movie_fields.map(lambda fields: fields[2]).map(
    lambda x: convert_year(x))
years_filtered = years.filter(lambda x: x != 1900)
movie_ages = years_filtered.map(lambda yr: 1998 - yr).countByValue()
values = movie_ages.values()
bins = movie_ages.keys()
# fig = plt.figure()
# ax = plt.hist(values, bins=bins, color='lightblue', normed=True)
# plt.save('')


# Rating data
rating_data = sc.textFile("file://" + os.path.abspath("../data/ml-100k") + "/u.data")
print(rating_data.first())
num_ratings = rating_data.count()
print("Ratings: %d" % num_ratings)

rating_data = rating_data.map(lambda line: line.split("\t"))
ratings = rating_data.map(lambda fields: int(fields[2]))
max_rating = ratings.reduce(lambda x, y: max(x, y))
min_rating = ratings.reduce(lambda x, y: min(x, y))
mean_rating = ratings.reduce(lambda x, y: x + y) / num_ratings
median_rating = np.median(ratings.collect())
ratings_per_user = num_ratings / num_users
ratings_per_movie = num_ratings / num_movies

print("MinRating, MaxRating = (%d, %d )" % (min_rating, max_rating))
print("Average Rating = %2.2f" % mean_rating)
print("Median rating = %d" % median_rating)
print("Average # of ratings per user: %2.2f" % ratings_per_user)
print("Average # of ratings per movie: %2.2f" % ratings_per_movie)
print(ratings.stats())
