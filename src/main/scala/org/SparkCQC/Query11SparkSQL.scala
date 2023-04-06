package org.SparkCQC

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * This is a test program for testing the following SQL query
 * SELECT T1.CA_ID, T1.S_SYMB, MAX(T2.T_TRADE_PRICE-T1.T_TRADE_PRICE)
 * FROM Trade T1, Trade T2
 * WHERE T1.TT = "BUY" AND T2.TT = "SALE" AND T1.CA_ID = T2.CA_ID AND T1.S_SYBM = T2.S_SYMB
 * AND T1.T_DTS <= T2.T_DTS AND T1.T_DTS + interval '90' day >= T2.T_DTS
 * GROUP BY T1.CA_ID, T1.S_SYBM
 */
object Query11SparkSQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Query11SparkSQL")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")

    assert(args.length >= 2)
    val path = args(0)
    val file = args(1)

    val lines = sc.textFile(s"file:${path}/${file}")
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
      "SELECT T1.CA_ID, T1.S_SYMB, MAX(T2.T_TRADE_PRICE-T1.T_TRADE_PRICE)\n" +
          "FROM Trade T1, Trade T2\n" +
          "WHERE T1.TT = \"BUY\" AND T2.TT = \"SALE\" AND T1.CA_ID = T2.CA_ID AND T1.S_SYBM = T2.S_SYMB\n" +
          "AND T1.T_DTS <= T2.T_DTS AND T1.T_DTS + interval '90' day >= T2.T_DTS\n" +
          "GROUP BY T1.CA_ID, T1.S_SYBM")

    spark.time(println(resultDF.count()))
    println("APP Name :" + spark.sparkContext.appName)
    println("Deploy Mode :" + spark.sparkContext.deployMode)
    println("Master :" + spark.sparkContext.master)

    spark.close()
  }
}
