from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, SQLContext


conf = SparkConf().setAppName("Spark Programming Guide").setMaster("local[*]")
sc = SparkContext(conf=conf).setLogLevel("WARN")
hc = HiveContext(sc)
