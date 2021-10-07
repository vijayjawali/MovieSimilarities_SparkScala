package com.vijay.spark


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, min}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}

object MinTemperatureDataset {

  case class Temperature(stationID: String, date: Int, measure_type: String, temperature: Float)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("MinTemperature")
      .master("local[*]")
      .getOrCreate()

    val temperatureSchema = new StructType()
      .add("stationID", StringType, nullable = true)
      .add("date", IntegerType, nullable = true)
      .add("measure_type", StringType, nullable = true)
      .add("temperature", FloatType, nullable = true)

    import spark.implicits._

    val ds = spark.read
      .schema(temperatureSchema)
      .csv("1800.csv")
      .as[Temperature]

    val minTemps = ds.filter(col("measure_type") === "TMIN")

    val stationTemps = minTemps.select(col("stationID"), col("temperature"))


    val minTempByStation = stationTemps.groupBy(col("stationID")).agg(min(col("temperature")))

    minTempByStation.show()


  }

}
