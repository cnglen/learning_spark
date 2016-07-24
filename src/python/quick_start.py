# ${SPARK_HOME}/bin/spark-submit quick_start.py
"""Quick Start"""

from pyspark import SparkContext

sc = SparkContext(appName="quick start")
textFile = sc.textFile("file:///opt/spark/README.md").cache()

# Action
print("Numer of lines: %d" % textFile.count())
print("First line: %s" % textFile.first())

# Transformation
lineWithSpark = textFile.filter(lambda line: "Spark" in line).cache()
print("Line with Spark: %s" % lineWithSpark.collect())
print("# of line with Spark: %d" % lineWithSpark.count())

print("Line with most words: %d" % textFile.map(lambda line: len(line.split())).reduce(lambda a, b: a if (a > b) else b))

wordCounts = textFile.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y)
print("Word counts: %s" % wordCounts.collect())

numAs = textFile.filter(lambda line: 'a' in line).count()
numBs = textFile.filter(lambda line: 'b' in line).count()
print("lines with a: %d, with b: %d" % (numAs, numBs))
