"""
Spark Streaming Programming Guide

Counts words in UTF8 encoded, '\n' delimted text received from the network every second.
Usage: network_wordcount.py <hostname> <port>

To run this on your local machine, you need to first run a NetCat server
`$ netcat -l -p 9999`
and then run the example
`$ $SPAKR_HOME/bin/spark-submit network_wordcount.py localhost 9999`

"""

from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: network_wordcount.py <hostname> <port>", file=sys.stderr)
        exit(-1)
    sc = SparkContext("local[2]", "NetworkWordCount")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 1)
    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    counts = lines.flatMap(lambda line: line.split(" "))\
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a+b)
    counts.pprint()
    ssc.start()
    ssc.awaitTermination()
