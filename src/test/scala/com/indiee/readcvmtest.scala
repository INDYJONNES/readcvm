package com.indiee

import org.apache.spark.sql.SparkSession
import org.scalatest.{ BeforeAndAfterAll, FunSuite}
import com.indiee.readcvm.loadDF
import com.indiee.readcvm.runsummary

import scala.collection.mutable

class readcvmtest extends FunSuite with BeforeAndAfterAll {
  @transient var spark : SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("Test read cvm").master("local[3]").getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }
  
  // first test case 
  test("Test Loading of Data") {
    val testDF = loadDF(spark,"data/sample.csv")
    val rCount = testDF.count()

    assert(rCount==9, " number of records must not be more than 9")
  }

  test("testing the summary") {
    val testDF = loadDF(spark, "data/sample.csv")
    val sumDF = runsummary(testDF)
    sumDF.show(true)
    // creating a hash mutable map object
    val hsummap = new mutable.HashMap[String,Long]
    sumDF.collect().foreach { r => hsummap.put(r.getString(0), r.getLong(1))}

    assert(hsummap("United States") == 4, ":- Count of United States = 4")
    assert(hsummap("Canada") == 2, ":- Count of Canada = 2")
    assert(hsummap("United Kingdom") == 1, ":- Count of United Kingdom = 1")
  }
  
}
