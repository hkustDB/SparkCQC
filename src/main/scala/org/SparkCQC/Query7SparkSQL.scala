package org.SparkCQC

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * This is a test program for testing the following SQL query
 * SELECT * FROM Trade T1, Trade T2, Trade T3
 * WHERE T1.CA_ID = T2.CA_ID
 * and T1.S_SYMB = T2.S_SYMB
 * and T2.CA_ID = T3.CA_ID
 * and T2.S_SYMB = T3.S_SYMB
 * and T1.T_DTS + interval '90' day <= T2.T_DTS
 * and T2.T_DTS + interval '90' day <= T3.T_DTS
 */
object Query7SparkSQL extends App {


  val conf = new SparkConf()
  conf.setAppName("Query5SparkSQL")
  val sc = new SparkContext(conf)

  val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
  val format = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")

  val lines = sc.textFile("Trade.txt")
  val db = lines.map(line => {
    val temp = line.split("\\|")
    (temp(0).toLong, temp(1).toLong, temp(2), temp(3), temp(4).toLong, temp(5).toDouble)
  }).coalesce(32)
  db.cache()
  spark.time(print(db.count()))

  //val graphSchemaString = "T_ID T_DTS T_TT_ID T_S_SYMB T_CA_ID T_TRADE_PRICE"
  val graphSchemaType = Array(("T_ID", LongType),
    ("T_DTS", LongType),
    ("T_TT_ID", StringType),
    ("T_S_SYMB", StringType),
    ("T_CA_ID", LongType),
    ("T_TRADE_PRICE", DoubleType)
  )

  val graphFields = graphSchemaType.map(x => StructField(x._1, x._2, nullable = false))
  val graphSchema = StructType(graphFields)


  val graphRow = db.map(attributes =>
    Row(attributes._1, attributes._2, attributes._3, attributes._4, attributes._5, attributes._6))

  val graphDF = spark.createDataFrame(graphRow, graphSchema)

  graphDF.createOrReplaceTempView("Trade")

  graphDF.persist()


  val resultDF = spark.sql(
    "SELECT DISTINCT T1.T_CA_ID, T1.T_S_SYMB \n" +
      "FROM Trade T1, Trade T2, Trade T3\n" +
      "WHERE T1.T_CA_ID = T2.T_CA_ID\n" +
      "and T1.T_S_SYMB = T2.T_S_SYMB\n " +
      "and T2.T_S_SYMB = T3.T_S_SYMB\n " +
      "and T2.T_CA_ID = T3.T_CA_ID\n " +
      "and T1.T_DTS + 7776000000 < T2.T_DTS\n " +
      "and T2.T_DTS + 7776000000 < T3.T_DTS;")

  spark.time(println(resultDF.count()))
  resultDF.explain("cost")
  println("First SparkContext:")
  println("APP Name :" + spark.sparkContext.appName)
  println("Deploy Mode :" + spark.sparkContext.deployMode)
  println("Master :" + spark.sparkContext.master)

  spark.close()
}
