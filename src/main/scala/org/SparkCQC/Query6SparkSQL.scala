package org.SparkCQC

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * This is a test program for testing the following SQL query
 * SELECT H1.CK, H2.CK, COUNT(DISTINCT H1.SK)
 * FROM Hold H1, Hold H2
 * WHERE H1.SK = H2. SK and H1.CK <> H2.CK
 * and H1.ST < H2.ET - interval '10' day
 * and H2.ST < H1.ET - interval '10' day
 * GROUP BY H1.CK, H2.CK
 */
object Query6SparkSQL extends App {


  val conf = new SparkConf()
  conf.setAppName("Query6SparkSQL")
  val sc = new SparkContext(conf)

  val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
  val format = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")

  val lines = sc.textFile("Holding.txt",1).coalesce(1)

  val db = lines.map(line => {
    val temp = line.split("\\|")
    (temp(0).toLong, temp(1), new java.sql.Date(format.parse(temp(2)).getTime), new java.sql.Date(format.parse(temp(3)).getTime))
  }).coalesce(32)
  db.cache()
  spark.time(print(db.count()))

  //val graphSchemaString = "CustKey StockKey StartTime EndTime"
  val graphSchemaType = Array(("CK", LongType),
    ("SK", StringType),
    ("ST", DateType),
    ("ET", DateType)
  )

  val graphFields = graphSchemaType.map(x => StructField(x._1, x._2, nullable = false))
  val graphSchema = StructType(graphFields)


  val graphRow = db.map(attributes =>
    Row(attributes._1, attributes._2, attributes._3, attributes._4))

  val graphDF = spark.createDataFrame(graphRow, graphSchema)

  graphDF.createOrReplaceTempView("Hold")

  graphDF.persist()
  spark.time(println(graphDF.count()))



  val resultDF = spark.sql(
    "SELECT H1.CK, H2.CK, COUNT(Distinct H1.SK) as CNT \n" +
  "FROM Hold H1, Hold H2 \n" +
  "WHERE H1.SK = H2.SK and H1.CK <> H2.CK \n"+
  "and H1.ST < H2.ET - interval '10' day \n" +
  "and H2.ST < H1.ET - interval '10' day \n" +
  "GROUP BY H1.CK, H2.CK")

  spark.time(println(resultDF.count()))
  resultDF.explain("cost")
  println("First SparkContext:")
  println("APP Name :" + spark.sparkContext.appName)
  println("Deploy Mode :" + spark.sparkContext.deployMode)
  println("Master :" + spark.sparkContext.master)

  spark.close()
}
