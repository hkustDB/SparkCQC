package org.SparkCQC

import org.SparkCQC.ComparisonJoins._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * This is a test program for testing the following SQL query
 * SELECT DISTINCT T1.T_CA_ID, T1.T_S_SYMB FROM Trade T1, Trade T2, Trade T3
 * WHERE T1.CA_ID = T2.CA_ID
 * and T1.S_SYMB = T2.S_SYMB
 * and T2.CA_ID = T3.CA_ID
 * and T2.S_SYMB = T3.S_SYMB
 * and T1.T_DTS + interval '90' day <= T2.T_DTS
 * and T2.T_DTS + interval '90' day <= T3.T_DTS
 */
object Query7SparkCQC extends App {

  val conf = new SparkConf()
  conf.setAppName("Query7SparkCQC")
  val sc = new SparkContext(conf)

  val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
  val format = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")
  val lines = sc.textFile("Trade.txt")


  val partitioner = new HashPartitioner(32)

  val dba = sc.textFile("Trade.txt").coalesce(32).map(line => {
    val temp = line.split("\\|")
    ((temp(3), temp(4).toLong), Array(temp(0).toLong, temp(1).toLong + 7776000000L, temp(3), temp(4).toLong, temp(5).toDouble))
  }).partitionBy(partitioner).cache()

  val dbb = sc.textFile("Trade.txt").coalesce(32).map(line => {
    val temp = line.split("\\|")
    ((temp(3), temp(4).toLong), Array(temp(0).toLong, temp(1).toLong, temp(3), temp(4).toLong, temp(5).toDouble))
  }).partitionBy(partitioner).cache()

  val dbc = sc.textFile("Trade.txt").coalesce(32).map(line => {
    val temp = line.split("\\|")
    ((temp(3), temp(4).toLong), Array(temp(0).toLong, temp(1).toLong - 7776000000L, temp(3), temp(4).toLong, temp(5).toDouble))
  }).partitionBy(partitioner).cache()

  spark.time(println(dbb.count()))
  spark.time(println(dba.count()))
  spark.time(println(dbc.count()))

  def largerL(x: Long, y: Long): Boolean = {

    if (x > y) true
    else false
  }

  def smallerL(x: Long, y: Long): Boolean = {

    if (x < y) true
    else false
  }

  def smallerD(x: Double, y: Double): Boolean = {
    if (x < y) true
    else false
  }

  val C = new ComparisonJoins()


  val dbaMax = C.getMinimal(dba, 1, smallerL)

  val dbbSemiJoin1 = C.enumerationnp(dbaMax, dbb, Array(), Array(0, 1, 2, 3, 4), (1, 0), (2, 1), smallerL(_, _))

  val dbbSemiJoinMinimal = C.getMinimal(dbbSemiJoin1, 1, smallerL)

  val dbcMax = C.getMinimal(dbc, 1, largerL)

  val dbbSemiJoin2 = C.enumerationnp(dbcMax, dbbSemiJoinMinimal, Array(), Array(0), (1, 0), (2, 0), largerL(_, _))

  val result = dbbSemiJoin2.keys

  spark.time(println(result.count()))

  println("First SparkContext:")
  println("APP Name :" + spark.sparkContext.appName)
  println("Deploy Mode :" + spark.sparkContext.deployMode)
  println("Master :" + spark.sparkContext.master)

  spark.close()
}
