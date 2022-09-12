package org.SparkCQC

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * This is a test program for testing the following SQL query over a graph
 * SELECT g1.src, g2.src, g3.src, g4.src, g5.src, g6.src
 * From Graph g1, Graph g2, Graph g3, Graph g4, Graph g5, Graph g6, Graph g7
 * where g1.src = g3.dst and g2.src = g1.dst and g3.src=g2.dst
 * and g4.src = g6.dst and g5.src = g4.dst and g6.src = g5.dst
 * and g1.dst = g7.src and g4.src = g7.dst and
 * g1.weight*g2.weight*g3.weight + k < g4.weight*g5.weight*g6.weight
 */
object Query2SparkSQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Query2SparkSQL")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    assert(args.length >= 2)
    val path = args(0)
    val file = args(1)
    val k = args(2).toInt

    val lines = sc.textFile(s"file:${path}/${file}")
    val graph = lines.map(line => {
      val temp = line.split(",")
      (temp(0).toInt, temp(1).toInt, temp(2).toInt)
    })
    graph.cache()


    val graphSchemaString = "src dst weight"
    val graphFields = graphSchemaString.split(" ")
        .map(fieldName => StructField(fieldName, IntegerType, nullable = false))
    val graphSchema = StructType(graphFields)


    val graphRow = graph.map(attributes => Row(attributes._1, attributes._2, attributes._3))

    val graphDF = spark.createDataFrame(graphRow, graphSchema)

    graphDF.createOrReplaceTempView("Graph")

    graphDF.persist()


    val resultDF = spark.sql(
      "SELECT g1.src, g2.src, g3.src, g4.src, g5.src, g6.src " +
          "From Graph g1, Graph g2, Graph g3, Graph g4, Graph g5, Graph g6, Graph g7 " +
          "where g1.src = g3.dst and g2.src = g1.dst and g3.src=g2.dst " +
          "and g4.src = g6.dst and g5.src = g4.dst and g6.src = g5.dst " +
          s"and g1.dst = g7.src and g4.src = g7.dst and g1.weight*g2.weight*g3.weight + ${k} < g4.weight*g5.weight*g6.weight")

    spark.time(println(resultDF.count()))
    println("APP Name :" + spark.sparkContext.appName)
    println("Deploy Mode :" + spark.sparkContext.deployMode)
    println("Master :" + spark.sparkContext.master)

    spark.close()
  }
}
