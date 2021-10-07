package com.sundogsoftware.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkSQLDataset {
  case class Person(ID:Int, name:String, age:Int, numFriends:Int)

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession.builder
      .appName("SparkSQLDataset")
      .master("local[*]")
      .getOrCreate()



    // Infer the schema, and register the DataSet as a table.
    import spark.implicits._
    val schemaPeople = spark.read
      .option("header","false")
      .option("inferschema","true")
      .csv("fakefriends.csv")
      .withColumnRenamed("_c0","ID")
      .withColumnRenamed("_c1","name")
      .withColumnRenamed("_c2","age")
      .withColumnRenamed("_c3","numFriends")
      .as[Person]

    schemaPeople.printSchema()

    schemaPeople.createOrReplaceTempView("people")

    // SQL can be run over DataFrames that have been registered as a table
    val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

    val results = teenagers.collect()

    results.foreach(println)

    spark.stop()
  }

}
