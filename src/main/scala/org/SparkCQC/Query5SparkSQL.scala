package org.SparkCQC

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * This is a test program for testing the following SQL query over a graph
 * select g2.src, g2.dst
 * from Graph g1, Graph g2, Graph g3, Graph g4, Graph g5,
 * (select src, count(*) as cnt from Graph group by src) as c1,
 * (select src, count(*) as cnt from Graph group by src) as c2,
 * (select dst, count(*) as cnt from Graph group by dst) as c3,
 * (select dst, count(*) as cnt from Graph group by dst) as c4
 * where g1.dst = g2.src and g2.dst = g3.src and g1.src = c1.src
 * and g3.dst = c2.src and c1.cnt < c2.cnt
 * and g4.dst = g2.src and g2.dst = g5.src and g4.src = c3.dst
 * and g5.dst = c4.dst and c3.cnt < c4.cnt
 */
object Query5SparkSQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Query5SparkSQL")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    assert(args.length >= 2)
    val path = args(0)
    val file = args(1)

    val lines = sc.textFile(s"file:${path}/${file}")
    val graph = lines.map(line => {
      val temp = line.split("\\s+")
      (temp(0).toInt, temp(1).toInt)
    })
    val reversedGraph = graph.map(x => (x._2, x._1))
    graph.cache()
    graph.count()
    val frequency = graph.map(edge => (edge._1, 1)).reduceByKey((a, b) => a + b).cache()
    frequency.count()
    val frequency2 = graph.map(edge => (edge._2, 1)).reduceByKey((a, b) => a + b).cache()
    frequency2.count()

    val g1 = graph.join(frequency).map(x => (x._1, x._2._1, x._2._2)).cache()
    val g3 = graph.join(frequency2).map(x => (x._1, x._2._1, x._2._2)).cache()
    val g2 = reversedGraph.join(frequency).map(x => (x._2._1, x._1, x._2._2)).cache()
    val g4 = reversedGraph.join(frequency2).map(x => (x._2._1, x._1, x._2._2)).cache()


    val graphSchemaString = "src dst"
    val graphFields = graphSchemaString.split(" ")
        .map(fieldName => StructField(fieldName, IntegerType, nullable = false))
    val graphSchema = StructType(graphFields)

    val graphCountSchemaString = "src dst cnt"
    val graphCountFields = graphCountSchemaString.split(" ")
        .map(fieldName => StructField(fieldName, IntegerType, nullable = false))
    val graphCountSchema = StructType(graphCountFields)

    //  val countSchemaString = "src cnt"
    //  val countFields = countSchemaString.split(" ")
    //    .map(fieldName => StructField(fieldName, IntegerType, nullable = false))
    //  val countSchema = StructType(countFields)
    //
    //  val inDegreeSchemaString = "dst cnt"
    //  val inDegreeFields = inDegreeSchemaString.split(" ")
    //    .map(fieldName => StructField(fieldName, IntegerType, nullable = false))
    //  val inDegreeSchema = StructType(inDegreeFields)


    val graphRow = graph.map(attributes => Row(attributes._1, attributes._2))
    val g1Row = g1.map(attributes => Row(attributes._1, attributes._2, attributes._3))
    val g2Row = g2.map(attributes => Row(attributes._1, attributes._2, attributes._3))
    val g3Row = g3.map(attributes => Row(attributes._1, attributes._2, attributes._3))
    val g4Row = g4.map(attributes => Row(attributes._1, attributes._2, attributes._3))
    //  val countRow = frequency.map(attributes => Row(attributes._1, attributes._2))
    //  val inDegreeRow = frequency2.map(attributes => Row(attributes._1, attributes._2))

    val graphDF = spark.createDataFrame(graphRow, graphSchema)
    val g1DF = spark.createDataFrame(g1Row, graphCountSchema)
    val g2DF = spark.createDataFrame(g2Row, graphCountSchema)
    val g3DF = spark.createDataFrame(g3Row, graphCountSchema)
    val g4DF = spark.createDataFrame(g4Row, graphCountSchema)
    //  val countDF = spark.createDataFrame(countRow, countSchema)
    //  val inDegreeDF = spark.createDataFrame(inDegreeRow, inDegreeSchema)

    graphDF.createOrReplaceTempView("Graph")
    g1DF.createOrReplaceTempView("g1")
    g2DF.createOrReplaceTempView("g2")
    g3DF.createOrReplaceTempView("g3")
    g4DF.createOrReplaceTempView("g4")
    //  countDF.createOrReplaceTempView("countDF")
    //  inDegreeDF.createOrReplaceTempView("inDegree")

    graphDF.persist()
    g1.persist()
    g2.persist()
    g3.persist()
    g4.persist()
    //  countDF.persist()
    //  inDegreeDF.persist()
    println(graphDF.count())
    println(g1.count())
    println(g2.count())
    println(g3.count())
    println(g4.count())


    val resultDF = spark.sql(
      "SELECT distinct(g.src, g.dst) From g1, Graph g, g2, g3, g4 " +
          "where g1.dst = g.src and g.dst = g2.src and g1.cnt < g2.cnt " +
          "and g3.dst = g.src and g.dst = g4.src and g3.cnt < g4.cnt ")

    spark.time(println(resultDF.count()))

    println("APP Name :" + spark.sparkContext.appName)
    println("Deploy Mode :" + spark.sparkContext.deployMode)
    println("Master :" + spark.sparkContext.master)

    spark.close()
  }
}
