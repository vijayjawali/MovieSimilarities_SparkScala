package com.vijay.spark


import org.apache.log4j._
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, desc, length, max, regexp_extract, regexp_replace, sum, trim}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object MostPopularSuperheroDataset {

  case class SuperHeroNames(id:Int, name: String)
  case class SuperHero(value: String)

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("PopularSuperHero")
      .master("local[*]")
      .getOrCreate()

    val superHeroNameSchema = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("name",StringType, nullable = true)

    import spark.implicits._
    val names = spark.read
      .schema(superHeroNameSchema)
      .option("sep", " ")
      .csv("Marvel-names.txt")
      .as[SuperHeroNames]

    val lines = spark.read
      .textFile("Marvel-graph.txt")
      .as[SuperHero]

    val connections = lines.withColumn("id", functions.split(col("value"), " ").getItem(0))
      .withColumn("trim_value", trim(col("value")))
      .withColumn("connections", length(col("trim_value")) - length(regexp_replace(col("trim_value")," ","")))
      .groupBy("id")
      .agg(sum("connections").alias("connections"))

    val maxConnections = connections.orderBy(desc("connections")).limit(1)

    val superheroName =  maxConnections.join(names,maxConnections("id") === names("id"),"inner").drop("id")

    superheroName.show()
  }

}
