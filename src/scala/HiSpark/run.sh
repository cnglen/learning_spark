sbt clean
sbt package
${SPARK_HOME}/bin/spark-submit --master local[2] --class HiSpark ./target/scala-2.10/hi-spark_2.10-0.1.jar
