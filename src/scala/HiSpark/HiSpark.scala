import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object HiSpark {

  def quickStart(sc: SparkContext) = {
    val sparkReadmeFile = sc.textFile("file://" + sys.env("SPARK_HOME") + "/README.md").cache()

    // Action
    println("Numer of lines: %d".format(sparkReadmeFile.count()))
    println("First line: %s".format(sparkReadmeFile.first()))

    // Transformation
    val lineWithSpark = sparkReadmeFile.filter(line => line.contains("Spark")).cache()
    println("Line with Spark: " + lineWithSpark.collect())
    println("# of line with Spark " + lineWithSpark.count())

    println("Line with most words: %d".format(sparkReadmeFile.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b)))

    val wordCounts = sparkReadmeFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((x, y) => x + y)
    print("Word counts: %s".format(wordCounts.collect()))

    val numAs = sparkReadmeFile.filter(line => line.contains("a")).count()
    val numBs = sparkReadmeFile.filter(line => line.contains("b")).count()
    println("lines with a: %d, with b: %d".format(numAs, numBs))
  }


  def simpleApp(sc: SparkContext) = {
    val data = sc.textFile("file://" + new java.io.File(".").getCanonicalPath + "/../../../data/user_purchase_history.csv")
      .map(line => line.split(","))
      .map(record => (record(0), record(1), record(2)))

    val numPurchases = data.count()
    // val uniqueUsers = data.map(x => x._1).distinct().count()
    val uniqueUsers = data.map{case (user, product, price) => user}.distinct().count()
    // val totalRevenue = data.map(record => record._3.toDouble).sum()
    val totalRevenue = data.map{case (user, product, price) => price.toDouble}.sum
    val productsByPopularity = data.map{case (user, product, price) => (product, 1)}.reduceByKey((x, y) => x+y).collect().sortBy(-_._2)
    val mostPopular = productsByPopularity(0)

    println("Total purchases: %d".format(numPurchases))
    println("Unique users: %d".format(uniqueUsers))
    println("Total revenue: %2.2f".format(totalRevenue))
    println("Most popular product: %s with %d purchases".format(mostPopular._1, mostPopular._2))
  }

  def main(args: Array[String]) = {
    val sc = new SparkContext("local[2]", "Spark demo app")
    sc.setLogLevel("WARN")

    quickStart(sc)

    simpleApp(sc)
  }
}
