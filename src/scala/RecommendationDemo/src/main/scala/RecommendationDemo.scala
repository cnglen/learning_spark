import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.DataFrame

object RecommendationSpark {

  def exploreData(sc: SparkContext) = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    // Note: run data/get_data.sh to get the data
    val userData = sc.textFile("file://" + new java.io.File(".").getCanonicalPath + "/../../../data/ml-100k/u.user") // RDD[String]
      .map(record => record.split('|')) // RDD[Array[String]]
      .map(r => (r(0).toInt, r(1).toInt, r(2), r(3), r(4))) // RDD[(Int, Int, String, String, String)]
      .toDF("user_id", "age", "gender", "occupation", "zip_code") // DataFrame
      .cache()
    val itemData = sc.textFile("file://" + new java.io.File(".").getCanonicalPath + "/../../../data/ml-100k/u.item")
      .map(record => record.split('|'))
      .map(r => (r(0).toInt, r(1), r(2), r(4)))
      .toDF("movie_id", "title", "release_date", "IMDB_link")
      .cache()
    val userItemRating = sc.textFile("file://" + new java.io.File(".").getCanonicalPath + "/../../../data/ml-100k/u.data")
      .map(record => record.split("\t"))
      .map(r => (r(0).toInt, r(1).toInt, r(2).toDouble, r(3).toInt))
      .toDF("user_id", "movie_id", "rating", "timestamp")
      .cache()

    // user data
    // user_id, age, gender, occupation, zip_code
    userData.printSchema
    userData.count
    println("n_users = %d, ")
    userData.describe().show
    for( index <- 0 until userData.columns.length) {
      println("Field: %s, NumberOfDistinctValues:%d".format(userData.columns(index), userData.map(row => row.get(index)).distinct().count()))
    }
    // item data
    // movie_id, title, release_date, IMDB_link

    def getStats(df: DataFrame) = {
      println("Schema:")
      df.printSchema
      println("Describe:")
      df.describe().show()
      println("Number of rows: %d" .format(df.count))
      for( index <- 0 until df.columns.length) {
        println("Field: %s, NumberOfDistinctValues:%d".format(df.columns(index), df.map(row => row.get(index)).distinct().count()))
      }
      // TBD
      // userData.map(row => row.getAs[Int]("age")).countByValue.toSeq.sorted.foreach(println)
    }
    userData.map(row => row.getAs[Int]("age")).countByValue.toSeq.sorted.foreach(println)
    getStats(userData)
    getStats(itemData)
    getStats(userItemRating)
  }

  def recommendationExample(sc: SparkContext) = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val rawData = sc.textFile("file://" + new java.io.File(".").getCanonicalPath + "/../../../data/ml-100k/u.data")
      .map(record => record.split("\t"))
      .map(r => (r(0).toInt, r(1).toInt, r(2).toDouble, r(3).toInt))
      .toDF("user_id", "movie_id", "rating", "timestamp")
      .cache()

    val rawRatings = rawData.select("user_id", "movie_id", "rating")
    import org.apache.spark.mllib.recommendation.ALS
    import org.apache.spark.mllib.recommendation.Rating
    val ratings = rawRatings.map(row => Rating(row.getAs[Int]("user_id"), row.getAs[Int]("movie_id"), row.getAs[Double]("rating")))

    // Training the model
    val model = ALS.train(ratings, rank=50, iterations=10, lambda=0.01)

    model.userFeatures.count
    model.productFeatures.count

    // Using the model
    val userId=789
    val productId=123
    model.predict(userId, productId)

    val K = 10
    val recommendedMovies = sc.parallelize(model.recommendProducts(userId, K)).toDF

    val rawMovies = sc.textFile("file://" + new java.io.File(".").getCanonicalPath + "/../../../data/ml-100k/u.item")
      .map(record => record.split('|'))
      .map(r => (r(0).toInt, r(1), r(2), r(4)))
      .toDF("movie_id", "title", "release_date", "IMDB_link")
      .cache()
    val titles = rawMovies.map(row => (row.getAs[Int]("movie_id"), row.getAs[String]("title"))).collectAsMap()
    val moviesRatedByUser = ratings.keyBy(_.user).lookup(userId)
    println("user %d rated %d movies".format(userId, moviesRatedByUser.size))
    moviesRatedByUser.sortBy(-_.rating)
    recommendedMovies.join(rawMovies, $"product"===$"movie_id", "left")

    // Print top movies rated by the user and recommended movies to the user
    moviesRatedByUser.sortBy(-_.rating).take(33).map(rating => (titles(rating.product), rating.rating)).foreach(println)
    recommendedMovies.map(row => (titles(row.getAs[Int]("product")), row.getAs[Double]("rating"))).foreach(println)
  }

  def main(args: Array[String]) = {
    val sc = new SparkContext("local[2]", "Spark demo app")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    sc.setLogLevel("WARN")

    exploreData(sc)
    recommendationExample(sc)
  }
}
