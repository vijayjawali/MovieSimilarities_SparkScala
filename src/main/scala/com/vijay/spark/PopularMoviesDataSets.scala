package com.vijay.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

/** Find the movies with the most ratings. */
object PopularMoviesDataSets {
  
  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, String] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()
    
     val lines = Source.fromFile("F:/Datasets/ml-100k/u.item").getLines()
     for (line <- lines) {
       var fields = line.split('|')
       if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
       }
     }
    
     return movieNames
  }
 
  // Case class so we can get a column name for our movie ID
  final case class Movie(movieID: Int)
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("PopularMovies")
      .master("local[*]")
      .getOrCreate()

    val movieSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)
    
    // Read in each rating line and extract the movie ID; construct a Movie objects.
    import spark.implicits._
    val moviesDS = spark.read
      .option("sep","\t")
      .schema(movieSchema)
      .csv("F:/Datasets/ml-100k/u.data")
      .as[Movie]
    
    // Some SQL-style magic to sort all movies by popularity in one line!
    val topMovieIDs = moviesDS.groupBy("movieID").count().orderBy(desc("count")).cache()
    
    // Show the results at this point:
    /*
    |movieID|count|
    +-------+-----+
    |     50|  584|
    |    258|  509|
    |    100|  508|   
    */
    
    topMovieIDs.show(10)
    
    // Grab the top 10
    val top10 = topMovieIDs.take(10)
    
    // Load up the movie ID -> name map
    val names = loadMovieNames()
    
    // Print the results
    println
    for (result <- top10) {
      // result is just a Row at this point; we need to cast it back.
      // Each row has movieID, count as above.
      println (names(result(0).asInstanceOf[Int]) + ": " + result(1))
    }

    // Stop the session
    spark.stop()
  }
  
}

