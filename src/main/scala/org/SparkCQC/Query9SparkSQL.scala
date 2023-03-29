package org.SparkCQC

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * This is a test program for testing the following SQL query over a graph
 * select g2.src, g2.dst, g3.dst, sum(c1.cnt)
 * from Graph g1, Graph g2, Graph g3,
 * (select src, count(*) as cnt from Graph group by src) as c1,
 * (select src, count(*) as cnt from Graph group by src) as c2
 * where g1.dst = g2.src and g2.dst = g3.src and g1.src = c1.src
 * and g3.dst = c2.src and c1.cnt < c2.cnt
 * group by g2.src, g2.dst, g3.dst
 */
object Query9SparkSQL {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.setAppName("Query9SparkSQL")
        val sc = new SparkContext(conf)

        val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

        assert(args.length >= 3)
        val path = args(0)
        val file = args(1)
        val k = args(2).toInt

        val lines = sc.textFile(s"file:${path}/${file}")
        val graph = lines.map(line => {
            val temp = line.split("\\s+")
            (temp(0).toInt, temp(1).toInt)
        })
        graph.cache()
        graph.count()
        val frequency = graph.map(edge => (edge._1, 1)).reduceByKey((a, b) => a + b).cache()
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

        val g1c1 = spark.sql("select g1.src AS src, g1.dst AS dst, c1.cnt AS cnt " +
            "from Graph g1, countDF c1 where g1.src = c1.src ")
        g1c1.createOrReplaceTempView("G1C1")
        g1c1.persist()
        g1c1.count()

        val g3c2 = spark.sql("select g3.src AS src, g3.dst AS dst, c2.cnt AS cnt " +
            s"from Graph g3, countDF c2 where g3.dst = c2.src ")
        g3c2.createOrReplaceTempView("G3C2")
        g3c2.persist()
        g3c2.count()

        val resultDF = spark.sql("select g2.src as a, g2.dst as b, g3c2.dst as c, sum(g1c1.cnt) " +
            "from G1C1 g1c1, Graph g2, G3C2 g3c2 " +
            "where g1c1.dst = g2.src and g2.dst = g3c2.src " +
            s"and g1c1.cnt < g3c2.cnt - ${k} group by g2.src, g2.dst, g3c2.dst")

        spark.time(println(resultDF.count()))
        println("APP Name :" + spark.sparkContext.appName)
        println("Deploy Mode :" + spark.sparkContext.deployMode)
        println("Master :" + spark.sparkContext.master)

        spark.close()
    }
}