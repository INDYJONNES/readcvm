package com.indiee

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame

import java.util.Properties
import scala.collection.JavaConverters.dictionaryAsScalaMapConverter
import scala.io.Source

object readcvm extends Serializable{

  def main(args: Array[String]): Unit = {
    
    // logging setting 
    @transient lazy val logger = Logger.getLogger(getClass.getName)
    
    logger.info("Starting Read CVM ")

    if (args.length == 0) {
      logger.error("Arguments expected")
      System.exit(1)
    }

    // creating spark session 
    val spark = SparkSession.builder()
      .config(setSparkConfig)
      .enableHiveSupport()
      .getOrCreate()

    logger.info("All Spark Configs "+ spark.conf.getAll.toString())
    
    // reading the sample csv file 
    val inDF = loadDF(spark,args(0))
    inDF.show()
    // printing in the logger editted
    logger.info(inDF.collect().mkString("->"))

    val summaryDF = runsummary(inDF)
    summaryDF.show()
    logger.info(summaryDF.collect.mkString("->"))

    logger.info("Ending CVM Reader")

    // close the spark session
    spark.stop()

  }

  def runsummary(dataFrame: DataFrame): DataFrame = {
    dataFrame.repartition(2).filter("Age < 40").select("Age","Gender","Country","state")
      .groupBy("Country")
      .count()
  }

  def loadDF(spark: SparkSession, datafile: String): DataFrame = {
    spark.read.option("header","true").option("inferSchema","true").csv(datafile)
  } // end of spark Dataframe creation

  def setSparkConfig(): SparkConf = {
    val customSparkConf = new SparkConf

    val cprops = new Properties

    cprops.load(Source.fromFile("spark.conf").bufferedReader())
    // cprops.forEach((k,v) => customSparkConf.set(k.toString, v.toString))

    // following code is for Scala 2.11
    import scala.collection.JavaConverters
    cprops.asScala.foreach(kv => customSparkConf.set(kv._1.toString, kv._2.toString))

    customSparkConf

  } // end of spark config function

}
