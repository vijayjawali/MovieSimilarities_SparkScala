package com.vijay.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.executor.DataReadMethod
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

object MovieSimilaritiesDataset {

  case class Movies(userId: Int, movieId: Int, rating: Int, timestamp: Long)
  case class MoviesNames(movieId: Int, movieTitle: String)
  case class MoviePairs(movie1: Int, movie2: Int, rating1: Int, rating2: Int)
  case class MoviePairsSimilarity(movie1: Int, movie2: Int, score: Double, numPairs: Long)


  def computeCosineSimilarity(spark: SparkSession, data: Dataset[MoviePairs]):Dataset[MoviePairsSimilarity] = {

    val pairScores = data
      .withColumn("xx", col("rating1") * col("rating1"))
      .withColumn("xy", col("rating1") * col("rating2"))
      .withColumn("yy", col("rating2") * col("rating2"))

    val calculateSimilarity = pairScores
      .groupBy("movie1", "movie2")
      .agg(sum(col("xy")).alias("numerator"),
        (sqrt(sum(col("xx"))) * sqrt(sum(col(("yy"))))).alias("denominator"),
        count(col("xy")).alias("numPairs"))

    import spark.implicits._

    val result = calculateSimilarity
      .withColumn("score",
        when(col("denominator") =!= 0, col("numerator")/col("denominator"))
          .otherwise(null))
      .select("movie1", "movie2", "score", "numPairs")
      .as[MoviePairsSimilarity]

    result

  }

  def getMovieName(movieNames: Dataset[MoviesNames], movieID: Int): String = {
    val result = movieNames.filter(col("movieID") === movieID)
      .select("movieTitle").collect()(0)

    result(0).toString
  }

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    // Logger.getLogger("org").setLevel(Level.ERROR)

    System.setProperty("hadoop.home.dir", "C:\\Hadoop\\")

    val spark = SparkSession.builder
      .appName("MovieSimilarities")
      .master("local[*]")
      .getOrCreate()

    val moviesNameSchema = new StructType()
      .add("movieId", IntegerType, nullable = true)
      .add("movieTitle", StringType, nullable = true)

    val moviesSchema = new StructType()
      .add("userId", IntegerType, nullable = true)
      .add("movieId", IntegerType, nullable = true)
      .add("rating",IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    println("\n Loading movie names...")

    import spark.implicits._

    val movieNames = spark.read
      .option("sep", "|")
      .option("charset","ISO-8859-1")
      .schema(moviesNameSchema)
      .csv("F:/Datasets/ml-100k/u.item")
      .as[MoviesNames]

    val movies = spark.read
      .option("sep","\t")
      .schema(moviesSchema)
      .csv("F:/Datasets/ml-100k/u.data")
      .as[Movies]

    val ratings = movies.select("userId", "movieId","rating")

    val moviePairs = ratings.as("ratings1")
      .join(ratings.as("ratings2"), 
        $"ratings1.userId" === $"ratings2.userId" &&  $"ratings1.movieId" < $"ratings2.movieId")
      .select($"ratings1.movieId".alias("movie1"),
        $"ratings2.movieId".alias("movie2"),
        $"ratings1.rating".alias("rating1"),
        $"ratings2.rating".alias("rating2")
      ).as[MoviePairs]

    val moviePairSimilarities = computeCosineSimilarity(spark, moviePairs).cache()

    if(args.length > 0) {
      val scoreThreshold = 0.97
      val coOccurenceThreshold = 50.0
      
      val movieID: Int = args(0).toInt
      
      val filteredResults = moviePairSimilarities
        .filter((col("movie1") === movieID || col("movie2") === movieID) 
          && (col("score") > scoreThreshold && col("numPairs") > coOccurenceThreshold))


      val results = filteredResults.sort(col("score").desc).take(10)
      
      println("\nTop 10 similar movies to " + getMovieName(movieNames, movieID))

      for(result <- results) {
        var similarMovieID = result.movie1
        if(similarMovieID == movieID) {
          similarMovieID = result.movie2
        }
        println(getMovieName(movieNames, similarMovieID) + "\t score: " + result.score + "\t strength: " + result.numPairs)
      }
    }
  }

}
