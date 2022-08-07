package org.SparkCQC

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * This is a test program for testing the following SQL query over a graph
 * select g1.src, g1.dst, g2.dst, g3.dst, c1.cnt, c2.cnt
 * from Graph g1, Graph g2, Graph g3,
 * (select src, count(*) as cnt from Graph group by src) as c1,
 * (select src, count(*) as cnt from Graph group by src) as c2
 * where g1.dst = g2.src and g2.dst = g3.src and g1.src = c1.src
 * and g3.dst = c2.src and c1.cnt + k < c2.cnt
 */
object Query1SparkSQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Query1SparkSQL")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    assert(args.length >= 3)
    val path = args(0)
    val file = args(1)
    val k = args(2).toInt

    val lines = sc.textFile(s"${path}/${file}")
    val graph = lines.map(line => {
      val temp = line.split("\\s+")
      (temp(0).toInt, temp(1).toInt)
    })
    graph.cache()
    graph.count()
    val frequency = graph.map(edge => (edge._1, 1)).reduceByKey((a, b) => a+b).cache()
    frequency.count()

    val graphSchemaString = "src dst"
    val graphFields = graphSchemaString.split(" ")
        .map(fieldName => StructField(fieldName, IntegerType, nullable = false))
    val graphSchema = StructType(graphFields)

    val countSchemaString = "src cnt"
    val countFields = countSchemaString.split(" ")
        .map(fieldName => StructField(fieldName, IntegerType, nullable = false))
    val countSchema = StructType(countFields)

    val graphRow = graph.map(attributes => Row(attributes._1, attributes._2))
    val countRow = frequency.map(attributes => Row(attributes._1, attributes._2))

    val graphDF = spark.createDataFrame(graphRow, graphSchema)
    val countDF = spark.createDataFrame(countRow, countSchema)

    graphDF.createOrReplaceTempView("Graph")
    countDF.createOrReplaceTempView("countDF")

    graphDF.persist()
    countDF.persist()
    graphDF.count()

    val resultDF = spark.sql(
      s"SELECT g1.src, g1.dst, g2.dst, g3.dst, c1.cnt, c2.cnt From Graph g1, Graph g2, Graph g3, countDF c1, countDF c2 " +
          s"where g1.dst = g2.src and g2.dst = g3.src and c1.src = g1.src and c2.src = g3.dst and c1.cnt + ${k} < c2.cnt")

    spark.time(println(resultDF.count()))
    println("APP Name :" + spark.sparkContext.appName)
    println("Deploy Mode :" + spark.sparkContext.deployMode)
    println("Master :" + spark.sparkContext.master)

    spark.close()
  }
}
