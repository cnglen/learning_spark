sbt clean
sbt package
${SPARK_HOME}/bin/spark-submit --master local[2] --class RecommendationSpark ./target/scala-2.10/recommendation-demo_2.10-0.1.jar
