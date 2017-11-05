package com.epam.hubd.spark.scala.sql.homework

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, UserDefinedFunction}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object MotelsHomeRecommendation {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    val sc = new SparkContext(new SparkConf().setAppName("motels-home-recommendation"))
    val sqlContext = new HiveContext(sc)

    processData(sqlContext, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  def processData(sqlContext: HiveContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: DataFrame = getRawBids(sqlContext, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      */
    val erroneousRecords: DataFrame = getErroneousRecords(rawBids)
    erroneousRecords.write
      .format(Constants.CSV_FORMAT)
      .save(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: DataFrame = getExchangeRates(sqlContext, exchangeRatesPath)

    /**
      * Task 3:
      * UserDefinedFunction to convert between date formats.
      * Hint: Check the formats defined in Constants class
      */
    val convertDate: UserDefinedFunction = getConvertDate

    /**
      * Task 3:
      * Transform the rawBids
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: DataFrame = getBids(rawBids, exchangeRates)

    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: DataFrame = getMotels(sqlContext, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names.
      */
    val enriched: DataFrame = getEnriched(bids, motels)
    enriched.write
      .format(Constants.CSV_FORMAT)
      .save(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(sqlContext: HiveContext, bidsPath: String): DataFrame =
    sqlContext.read.parquet(bidsPath)

  def getErroneousRecords(rawBids: DataFrame): DataFrame = {
    import rawBids.sqlContext.implicits._
    val result = rawBids
      .filter($"HU".substr(0, 6) === "ERROR_")
      .withColumnRenamed("HU","ErrorText")
      .select($"BidDate",$"ErrorText")
      .groupBy($"BidDate",$"ErrorText")
      .agg(count("*").alias("ErrorsCount"))
    return result
  }

  def getExchangeRates(sqlContext: HiveContext, exchangeRatesPath: String): DataFrame = {
    val exchangeRatesSchema = StructType(Array(
      StructField("ValidFrom", StringType, false),
      StructField("CurrencyName", StringType, false),
      StructField("CurrencyCode", StringType, false),
      StructField("ExchangeRate", DoubleType, false)))

    return sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false") // Use first line of all files as header
      .schema(exchangeRatesSchema)
      .load(exchangeRatesPath)
  }

  def getConvertDate: UserDefinedFunction = {
    def getConvertDate = (date: String) =>
      Constants.INPUT_DATE_FORMAT.parseDateTime(date).toString(Constants.OUTPUT_DATE_FORMAT)
    return udf(getConvertDate)
  }

  def getBids(rawBids: DataFrame, exchangeRates: DataFrame): DataFrame = {
    import rawBids.sqlContext.implicits._
    val er =
      exchangeRates
        .withColumn("ValidFrom",getConvertDate($"ValidFrom"))
        .select($"ValidFrom", $"ExchangeRate")

    return rawBids
      .filter($"HU".substr(0,6)!=="ERROR_")
      .withColumn("Bids",array(Constants.TARGET_LOSAS.map(x => rawBids(x)):_*))
      .explode[Seq[String],Seq[String]]("Bids","BidTuple")(
        x => Constants.TARGET_LOSAS
          .zip(x)
          .filter(x => !x._2.isEmpty)
          .map(x => Seq(x._1,x._2))
      )
      .withColumn("LoSA",$"BidTuple".getItem(0))
      .withColumn("Bid",$"BidTuple".getItem(1).cast(new DecimalType(16,3)))
      .filter(!$"Bid".isNull)
      .withColumn("BidDate",getConvertDate($"BidDate"))
      .select($"MotelID",$"BidDate",$"LoSA",$"Bid")
      .join(er, $"BidDate"===er("ValidFrom"))
      .withColumn("BidEUR",($"Bid"*$"ExchangeRate").cast(new DecimalType(16,3)))
      .select($"BidDate",$"LoSA",$"MotelID",$"BidEUR")
  }

  def getMotels(sqlContext: HiveContext, motelsPath: String): DataFrame =
    sqlContext.read.parquet(motelsPath)

  def getEnriched(bids: DataFrame, motels: DataFrame): DataFrame = {
    import bids.sqlContext.implicits._

    // I'm sorry for dirty hack, I'm too tired to search for proper solution
    def rtrimTwoZeroes = udf((x: String) => x.stripSuffix("0").stripSuffix("0"))

    val motelsNarrow = motels
      .select($"MotelID",$"MotelName")
      .withColumnRenamed("MotelID","MotelIDMotels")

    return bids
      .select($"BidDate",$"LoSA",$"MotelID",$"BidEUR")
      .withColumn("BidDay",$"BidDate".substr(0,10))
      .withColumn("MaxBidEUR",
        max($"BidEUR").over(
          Window.partitionBy($"BidDate",$"MotelID")
        ).cast(new DecimalType(16,3))
      )
      .filter($"BidEUR"===$"MaxBidEUR")
      .join(motelsNarrow,$"MotelID"===$"MotelIDMotels")
      .withColumn("BidEUR",rtrimTwoZeroes($"BidEUR".cast(StringType)))
      .select($"MotelID",$"MotelName",$"BidDate",$"LoSA",$"BidEUR")
  }
}
